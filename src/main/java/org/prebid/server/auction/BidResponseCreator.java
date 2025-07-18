package org.prebid.server.auction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.DataObject;
import com.iab.openrtb.request.ImageObject;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Native;
import com.iab.openrtb.request.Request;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Asset;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.Response;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.prebid.server.auction.categorymapping.CategoryMappingService;
import org.prebid.server.auction.model.AuctionContext;
import org.prebid.server.auction.model.AuctionParticipation;
import org.prebid.server.auction.model.BidInfo;
import org.prebid.server.auction.model.BidRequestCacheInfo;
import org.prebid.server.auction.model.BidderResponse;
import org.prebid.server.auction.model.BidderResponseInfo;
import org.prebid.server.auction.model.CachedDebugLog;
import org.prebid.server.auction.model.CategoryMappingResult;
import org.prebid.server.auction.model.MultiBidConfig;
import org.prebid.server.auction.model.PaaFormat;
import org.prebid.server.auction.model.TargetingInfo;
import org.prebid.server.auction.model.debug.DebugContext;
import org.prebid.server.auction.requestfactory.Ortb2ImplicitParametersResolver;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.BidderSeatBid;
import org.prebid.server.bidder.model.BidderSeatBidInfo;
import org.prebid.server.cache.CoreCacheService;
import org.prebid.server.cache.model.CacheContext;
import org.prebid.server.cache.model.CacheInfo;
import org.prebid.server.cache.model.CacheServiceResult;
import org.prebid.server.cache.model.CacheTtl;
import org.prebid.server.cache.model.DebugHttpCall;
import org.prebid.server.events.EventsContext;
import org.prebid.server.events.EventsService;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.timeout.Timeout;
import org.prebid.server.hooks.execution.HookStageExecutor;
import org.prebid.server.hooks.execution.model.HookStageExecutionResult;
import org.prebid.server.hooks.v1.bidder.AllProcessedBidResponsesPayload;
import org.prebid.server.hooks.v1.bidder.BidderResponsePayload;
import org.prebid.server.identity.IdGenerator;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.log.ConditionalLogger;
import org.prebid.server.log.Logger;
import org.prebid.server.log.LoggerFactory;
import org.prebid.server.metric.MetricName;
import org.prebid.server.metric.Metrics;
import org.prebid.server.proto.openrtb.ext.request.ExtImp;
import org.prebid.server.proto.openrtb.ext.request.ExtImpAuctionEnvironment;
import org.prebid.server.proto.openrtb.ext.request.ExtImpPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtMediaTypePriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtOptions;
import org.prebid.server.proto.openrtb.ext.request.ExtPriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidChannel;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestTargeting;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.CacheAsset;
import org.prebid.server.proto.openrtb.ext.response.Events;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidMeta;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidVideo;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponse;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponseFledge;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponsePrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidderError;
import org.prebid.server.proto.openrtb.ext.response.ExtDebugTrace;
import org.prebid.server.proto.openrtb.ext.response.ExtHttpCall;
import org.prebid.server.proto.openrtb.ext.response.ExtIgi;
import org.prebid.server.proto.openrtb.ext.response.ExtIgiIgb;
import org.prebid.server.proto.openrtb.ext.response.ExtIgiIgs;
import org.prebid.server.proto.openrtb.ext.response.ExtIgiIgsExt;
import org.prebid.server.proto.openrtb.ext.response.ExtResponseCache;
import org.prebid.server.proto.openrtb.ext.response.ExtResponseDebug;
import org.prebid.server.proto.openrtb.ext.response.ExtTraceActivityInfrastructure;
import org.prebid.server.proto.openrtb.ext.response.FledgeAuctionConfig;
import org.prebid.server.proto.openrtb.ext.response.seatnonbid.NonBid;
import org.prebid.server.proto.openrtb.ext.response.seatnonbid.SeatNonBid;
import org.prebid.server.settings.model.Account;
import org.prebid.server.settings.model.AccountAnalyticsConfig;
import org.prebid.server.settings.model.AccountAuctionConfig;
import org.prebid.server.settings.model.AccountAuctionEventConfig;
import org.prebid.server.settings.model.AccountBidRankingConfig;
import org.prebid.server.settings.model.AccountEventsConfig;
import org.prebid.server.settings.model.AccountTargetingConfig;
import org.prebid.server.settings.model.VideoStoredDataResult;
import org.prebid.server.spring.config.model.CacheDefaultTtlProperties;
import org.prebid.server.util.ListUtil;
import org.prebid.server.util.StreamUtil;
import org.prebid.server.vast.VastModifier;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BidResponseCreator {

    private static final Logger logger = LoggerFactory.getLogger(BidResponseCreator.class);
    private static final ConditionalLogger conditionalLogger = new ConditionalLogger(logger);

    private static final String CACHE = "cache";
    private static final String PREBID_EXT = "prebid";
    private static final Integer DEFAULT_BID_LIMIT_MIN = 1;
    private static final Integer MAX_TARGETING_KEY_LENGTH = 11;
    private static final String DEFAULT_TARGETING_KEY_PREFIX = "hb";
    public static final String DEFAULT_DEBUG_KEY = "prebid";
    private static final String TARGETING_ENV_APP_VALUE = "mobile-app";
    private static final String TARGETING_ENV_AMP_VALUE = "amp";
    private static final int MIN_BID_ID_LENGTH = 17;

    private final double logSamplingRate;
    private final CoreCacheService coreCacheService;
    private final BidderCatalog bidderCatalog;
    private final VastModifier vastModifier;
    private final EventsService eventsService;
    private final StoredRequestProcessor storedRequestProcessor;
    private final WinningBidComparatorFactory winningBidComparatorFactory;
    private final IdGenerator bidIdGenerator;
    private final IdGenerator enforcedBidIdGenerator;
    private final HookStageExecutor hookStageExecutor;
    private final CategoryMappingService categoryMappingService;
    private final int truncateAttrChars;
    private final boolean enforceRandomBidId;
    private final Clock clock;
    private final JacksonMapper mapper;
    private final Metrics metrics;
    private final CacheTtl mediaTypeCacheTtl;
    private final CacheDefaultTtlProperties cacheDefaultProperties;

    private final String cacheHost;
    private final String cachePath;
    private final String cacheAssetUrlTemplate;

    public BidResponseCreator(double logSamplingRate,
                              CoreCacheService coreCacheService,
                              BidderCatalog bidderCatalog,
                              VastModifier vastModifier,
                              EventsService eventsService,
                              StoredRequestProcessor storedRequestProcessor,
                              WinningBidComparatorFactory winningBidComparatorFactory,
                              IdGenerator bidIdGenerator,
                              IdGenerator enforcedBidIdGenerator,
                              HookStageExecutor hookStageExecutor,
                              CategoryMappingService categoryMappingService,
                              int truncateAttrChars,
                              boolean enforceRandomBidId,
                              Clock clock,
                              JacksonMapper mapper,
                              Metrics metrics,
                              CacheTtl mediaTypeCacheTtl,
                              CacheDefaultTtlProperties cacheDefaultProperties) {

        this.coreCacheService = Objects.requireNonNull(coreCacheService);
        this.bidderCatalog = Objects.requireNonNull(bidderCatalog);
        this.vastModifier = Objects.requireNonNull(vastModifier);
        this.eventsService = Objects.requireNonNull(eventsService);
        this.storedRequestProcessor = Objects.requireNonNull(storedRequestProcessor);
        this.winningBidComparatorFactory = Objects.requireNonNull(winningBidComparatorFactory);
        this.bidIdGenerator = Objects.requireNonNull(bidIdGenerator);
        this.enforcedBidIdGenerator = Objects.requireNonNull(enforcedBidIdGenerator);
        this.hookStageExecutor = Objects.requireNonNull(hookStageExecutor);
        this.categoryMappingService = Objects.requireNonNull(categoryMappingService);
        this.truncateAttrChars = validateTruncateAttrChars(truncateAttrChars);
        this.enforceRandomBidId = enforceRandomBidId;
        this.clock = Objects.requireNonNull(clock);
        this.mapper = Objects.requireNonNull(mapper);
        this.mediaTypeCacheTtl = Objects.requireNonNull(mediaTypeCacheTtl);
        this.cacheDefaultProperties = Objects.requireNonNull(cacheDefaultProperties);
        this.metrics = Objects.requireNonNull(metrics);
        this.logSamplingRate = logSamplingRate;

        cacheAssetUrlTemplate = Objects.requireNonNull(coreCacheService.getCachedAssetURLTemplate());
        cacheHost = Objects.requireNonNull(coreCacheService.getEndpointHost());
        cachePath = Objects.requireNonNull(coreCacheService.getEndpointPath());
    }

    private static int validateTruncateAttrChars(int truncateAttrChars) {
        if (truncateAttrChars < 0 || truncateAttrChars > 255) {
            throw new IllegalArgumentException("truncateAttrChars must be between 0 and 255");
        }
        return truncateAttrChars;
    }

    Future<BidResponse> create(AuctionContext auctionContext,
                               BidRequestCacheInfo cacheInfo,
                               Map<String, MultiBidConfig> bidderToMultiBids) {

        return videoStoredDataResult(auctionContext)
                .compose(videoStoredData ->
                        create(videoStoredData, auctionContext, cacheInfo, bidderToMultiBids))
                .map(bidResponse -> populateSeatNonBid(auctionContext, bidResponse));
    }

    private Future<BidResponse> create(VideoStoredDataResult videoStoredDataResult,
                                       AuctionContext auctionContext,
                                       BidRequestCacheInfo cacheInfo,
                                       Map<String, MultiBidConfig> bidderToMultiBids) {

        final EventsContext eventsContext = createEventsContext(auctionContext);
        final List<BidderResponse> bidderResponses = auctionContext.getAuctionParticipations().stream()
                .filter(auctionParticipation -> !auctionParticipation.isRequestBlocked())
                .map(AuctionParticipation::getBidderResponse)
                .toList();

        return updateBids(bidderResponses, videoStoredDataResult, auctionContext, eventsContext)
                .compose(updatedResponses -> invokeProcessedBidderResponseHooks(updatedResponses, auctionContext))
                .compose(updatedResponses -> invokeAllProcessedBidResponsesHook(updatedResponses, auctionContext))
                .compose(updatedResponses -> createCategoryMapping(auctionContext, updatedResponses))
                .compose(categoryMappingResult -> cacheBidsAndCreateResponse(
                        toBidderResponseInfos(categoryMappingResult, cacheInfo, auctionContext),
                        auctionContext,
                        cacheInfo,
                        bidderToMultiBids,
                        videoStoredDataResult,
                        eventsContext));
    }

    private Future<List<BidderResponse>> updateBids(List<BidderResponse> bidderResponses,
                                                    VideoStoredDataResult videoStoredDataResult,
                                                    AuctionContext auctionContext,
                                                    EventsContext eventsContext) {

        final List<BidderResponse> result = new ArrayList<>();

        for (final BidderResponse bidderResponse : bidderResponses) {
            final String bidder = bidderResponse.getBidder();
            final List<BidderBid> modifiedBidderBids = new ArrayList<>();
            final BidderSeatBid seatBid = bidderResponse.getSeatBid();
            for (final BidderBid bidderBid : seatBid.getBids()) {
                final Bid receivedBid = bidderBid.getBid();
                final BidType bidType = bidderBid.getType();

                final Bid updatedBid = updateBid(
                        receivedBid, bidType, bidder, videoStoredDataResult, auctionContext, eventsContext);
                modifiedBidderBids.add(bidderBid.toBuilder().bid(updatedBid).build());
            }

            final BidderSeatBid modifiedSeatBid = seatBid.with(modifiedBidderBids);
            result.add(bidderResponse.with(modifiedSeatBid));
        }

        return Future.succeededFuture(result);
    }

    private Bid updateBid(Bid bid,
                          BidType bidType,
                          String bidder,
                          VideoStoredDataResult videoStoredDataResult,
                          AuctionContext auctionContext,
                          EventsContext eventsContext) {

        final Account account = auctionContext.getAccount();
        final List<String> debugWarnings = auctionContext.getDebugWarnings();

        final String generatedBidId = bidIdGenerator.generateId();
        final String enforcedRandomBidId = enforcedBidId(bid);
        final String effectiveBidId = ObjectUtils.defaultIfNull(generatedBidId, enforcedRandomBidId);

        return bid.toBuilder()
                .id(enforcedRandomBidId)
                .adm(updateBidAdm(bid,
                        bidType,
                        bidder,
                        account,
                        eventsContext,
                        effectiveBidId,
                        debugWarnings))
                .ext(updateBidExt(
                        bid,
                        bidType,
                        bidder,
                        account,
                        videoStoredDataResult,
                        eventsContext,
                        generatedBidId,
                        effectiveBidId))
                .build();
    }

    private String enforcedBidId(Bid bid) {
        final int bidIdLength = Optional.ofNullable(bid.getId()).map(String::length).orElse(0);
        return enforceRandomBidId && bidIdLength < MIN_BID_ID_LENGTH
                ? enforcedBidIdGenerator.generateId()
                : bid.getId();
    }

    private String updateBidAdm(Bid bid,
                                BidType bidType,
                                String bidder,
                                Account account,
                                EventsContext eventsContext,
                                String effectiveBidId,
                                List<String> debugWarnings) {

        final String bidAdm = bid.getAdm();
        return BidType.video.equals(bidType)
                ? vastModifier.createBidVastXml(
                bidder,
                bidAdm,
                bid.getNurl(),
                effectiveBidId,
                account.getId(),
                eventsContext,
                debugWarnings)
                : bidAdm;
    }

    private ObjectNode updateBidExt(Bid bid,
                                    BidType bidType,
                                    String bidder,
                                    Account account,
                                    VideoStoredDataResult videoStoredDataResult,
                                    EventsContext eventsContext,
                                    String generatedBidId,
                                    String effectiveBidId) {

        final ExtBidPrebid updatedExtBidPrebid = updateBidExtPrebid(
                bid,
                bidType,
                bidder,
                account,
                videoStoredDataResult,
                eventsContext,
                generatedBidId,
                effectiveBidId);
        final ObjectNode existingBidExt = bid.getExt();
        final ObjectNode updatedBidExt = mapper.mapper().createObjectNode();

        if (existingBidExt != null && !existingBidExt.isEmpty()) {
            updatedBidExt.setAll(existingBidExt);
        }

        updatedBidExt.set(PREBID_EXT, mapper.mapper().valueToTree(updatedExtBidPrebid));
        return updatedBidExt;
    }

    private ExtBidPrebid updateBidExtPrebid(Bid bid,
                                            BidType bidType,
                                            String bidder,
                                            Account account,
                                            VideoStoredDataResult videoStoredDataResult,
                                            EventsContext eventsContext,
                                            String generatedBidId,
                                            String effectiveBidId) {

        final Video storedVideo = videoStoredDataResult.getImpIdToStoredVideo().get(bid.getImpid());
        final Events events = createEvents(bidder, account, effectiveBidId, eventsContext);
        final ExtBidPrebidVideo extBidPrebidVideo = getExtBidPrebidVideo(bid.getExt()).orElse(null);
        final ExtBidPrebid.ExtBidPrebidBuilder extBidPrebidBuilder = getExtPrebid(bid.getExt(), ExtBidPrebid.class)
                .map(ExtBidPrebid::toBuilder)
                .orElseGet(ExtBidPrebid::builder);

        return extBidPrebidBuilder
                .bidid(generatedBidId)
                .type(bidType)
                .storedRequestAttributes(storedVideo)
                .events(events)
                .video(extBidPrebidVideo)
                .build();
    }

    /**
     * Checks whether bidder responses are empty or contain no bids.
     */
    private static boolean isEmptyBidderResponses(List<BidderResponseInfo> bidderResponseInfos) {
        return bidderResponseInfos.isEmpty() || bidderResponseInfos.stream()
                .map(bidderResponseInfo -> bidderResponseInfo.getSeatBid().getBidsInfos())
                .allMatch(CollectionUtils::isEmpty);
    }

    private List<BidderResponseInfo> toBidderResponseInfos(CategoryMappingResult categoryMappingResult,
                                                           BidRequestCacheInfo cacheInfo,
                                                           AuctionContext auctionContext) {

        final List<Imp> imps = auctionContext.getBidRequest().getImp();
        final Account account = auctionContext.getAccount();
        final List<BidderResponseInfo> result = new ArrayList<>();
        final List<BidderResponse> bidderResponses = categoryMappingResult.getBidderResponses();

        for (final BidderResponse bidderResponse : bidderResponses) {
            final String bidder = bidderResponse.getBidder();
            final BidderSeatBid seatBid = bidderResponse.getSeatBid();

            final Map<String, List<BidderBid>> seatToBids = seatBid.getBids().stream()
                    .filter(bidderBid -> Objects.nonNull(bidderBid.getSeat()))
                    .collect(Collectors.groupingBy(BidderBid::getSeat));

            if (seatToBids.isEmpty()) {
                final BidderSeatBidInfo bidderSeatBidInfo = BidderSeatBidInfo.of(
                        Collections.emptyList(),
                        seatBid.getHttpCalls(),
                        seatBid.getErrors(),
                        seatBid.getWarnings(),
                        seatBid.getFledgeAuctionConfigs(),
                        seatBid.getIgi());

                result.add(BidderResponseInfo.of(
                        bidder,
                        bidder,
                        bidder,
                        bidderSeatBidInfo,
                        bidderResponse.getResponseTime()));

                continue;
            }

            for (Map.Entry<String, List<BidderBid>> bidsEntry : seatToBids.entrySet()) {
                final List<BidInfo> bidInfos = new ArrayList<>();
                final String seat = bidsEntry.getKey();
                final List<BidderBid> bids = bidsEntry.getValue();
                final BidderBid firstBid = CollectionUtils.isEmpty(bids) ? null : bids.getFirst();
                final String adapterCode = Optional.ofNullable(firstBid)
                        .map(BidderBid::getBid)
                        .map(Bid::getExt)
                        .flatMap(ext -> getExtPrebid(ext, ExtBidPrebid.class))
                        .map(ExtBidPrebid::getMeta)
                        .map(ExtBidPrebidMeta::getAdapterCode)
                        .orElse(bidder);

                for (final BidderBid bidderBid : bids) {
                    final BidInfo bidInfo = toBidInfo(
                            bidderBid.getBid(),
                            bidderBid.getType(),
                            seat,
                            imps,
                            bidder,
                            categoryMappingResult,
                            cacheInfo,
                            account);
                    bidInfos.add(bidInfo);
                }

                final BidderSeatBidInfo bidderSeatBidInfo = BidderSeatBidInfo.of(
                        bidInfos,
                        seatBid.getHttpCalls(),
                        seatBid.getErrors(),
                        seatBid.getWarnings(),
                        seatBid.getFledgeAuctionConfigs(),
                        seatBid.getIgi());

                result.add(BidderResponseInfo.of(
                        bidder,
                        seat,
                        adapterCode,
                        bidderSeatBidInfo,
                        bidderResponse.getResponseTime()));
            }
        }
        return result;
    }

    private BidInfo toBidInfo(Bid bid,
                              BidType type,
                              String seat,
                              List<Imp> imps,
                              String bidder,
                              CategoryMappingResult categoryMappingResult,
                              BidRequestCacheInfo cacheInfo,
                              Account account) {

        final Imp correspondingImp = correspondingImp(bid, imps);
        return BidInfo.builder()
                .bid(bid)
                .bidType(type)
                .bidder(bidder)
                .seat(seat)
                .correspondingImp(correspondingImp)
                .ttl(resolveTtl(bid, type, correspondingImp, cacheInfo, account))
                .vastTtl(type == BidType.video ? resolveVastTtl(bid, correspondingImp, cacheInfo, account) : null)
                .category(categoryMappingResult.getCategory(bid))
                .satisfiedPriority(categoryMappingResult.isBidSatisfiesPriority(bid))
                .build();
    }

    private static Imp correspondingImp(Bid bid, List<Imp> imps) {
        final String impId = bid.getImpid();
        return correspondingImp(impId, imps)
                // Should never occur. See ResponseBidValidator
                .orElseThrow(
                        () -> new PreBidException("Bid with impId %s doesn't have matched imp".formatted(impId)));
    }

    private static Optional<Imp> correspondingImp(String impId, List<Imp> imps) {
        return imps.stream()
                .filter(imp -> Objects.equals(impId, imp.getId()))
                .findFirst();
    }

    private Integer resolveTtl(Bid bid, BidType type, Imp imp, BidRequestCacheInfo cacheInfo, Account account) {
        final Integer bidTtl = bid.getExp();
        final Integer impTtl = imp != null ? imp.getExp() : null;
        final Integer requestTtl = cacheInfo.getCacheBidsTtl();

        final AccountAuctionConfig accountAuctionConfig = account.getAuction();
        final Integer accountTtl = accountAuctionConfig != null ? switch (type) {
            case banner -> accountAuctionConfig.getBannerCacheTtl();
            case video -> accountAuctionConfig.getVideoCacheTtl();
            case audio, xNative -> null;
        } : null;

        final Integer mediaTypeTtl = switch (type) {
            case banner -> mediaTypeCacheTtl.getBannerCacheTtl();
            case video -> mediaTypeCacheTtl.getVideoCacheTtl();
            case audio, xNative -> null;
        };

        final Integer defaultTtl = switch (type) {
            case banner -> cacheDefaultProperties.getBannerTtl();
            case video -> cacheDefaultProperties.getVideoTtl();
            case audio -> cacheDefaultProperties.getAudioTtl();
            case xNative -> cacheDefaultProperties.getNativeTtl();
        };

        return ObjectUtils.firstNonNull(bidTtl, impTtl, requestTtl, accountTtl, mediaTypeTtl, defaultTtl);
    }

    private Integer resolveVastTtl(Bid bid, Imp imp, BidRequestCacheInfo cacheInfo, Account account) {
        final AccountAuctionConfig accountAuctionConfig = account.getAuction();
        return ObjectUtils.firstNonNull(
                bid.getExp(),
                imp != null ? imp.getExp() : null,
                cacheInfo.getCacheVideoBidsTtl(),
                accountAuctionConfig != null ? accountAuctionConfig.getVideoCacheTtl() : null,
                mediaTypeCacheTtl.getVideoCacheTtl(),
                cacheDefaultProperties.getVideoTtl());
    }

    private Future<List<BidderResponse>> invokeProcessedBidderResponseHooks(List<BidderResponse> bidderResponses,
                                                                            AuctionContext auctionContext) {

        return Future.join(bidderResponses.stream()
                        .map(bidderResponse -> hookStageExecutor
                                .executeProcessedBidderResponseStage(bidderResponse, auctionContext)
                                .map(stageResult -> rejectBidderResponseOrProceed(stageResult, bidderResponse)))
                        .toList())
                .map(CompositeFuture::list);
    }

    private Future<List<BidderResponse>> invokeAllProcessedBidResponsesHook(List<BidderResponse> bidderResponses,
                                                                            AuctionContext auctionContext) {

        return hookStageExecutor.executeAllProcessedBidResponsesStage(bidderResponses, auctionContext)
                .map(HookStageExecutionResult::getPayload)
                .map(AllProcessedBidResponsesPayload::bidResponses);
    }

    private static BidderResponse rejectBidderResponseOrProceed(
            HookStageExecutionResult<BidderResponsePayload> stageResult,
            BidderResponse bidderResponse) {

        final List<BidderBid> bids = stageResult.isShouldReject()
                ? Collections.emptyList()
                : stageResult.getPayload().bids();

        return bidderResponse.with(bidderResponse.getSeatBid().with(bids));
    }

    private Future<CategoryMappingResult> createCategoryMapping(AuctionContext auctionContext,
                                                                List<BidderResponse> bidderResponses) {

        return categoryMappingService.createCategoryMapping(
                        bidderResponses,
                        auctionContext.getBidRequest(),
                        auctionContext.getAccount(),
                        auctionContext.getTimeoutContext().getTimeout())

                .map(categoryMappingResult -> addCategoryMappingErrors(categoryMappingResult, auctionContext));
    }

    private static CategoryMappingResult addCategoryMappingErrors(CategoryMappingResult categoryMappingResult,
                                                                  AuctionContext auctionContext) {

        auctionContext.getPrebidErrors()
                .addAll(CollectionUtils.emptyIfNull(categoryMappingResult.getErrors()));

        return categoryMappingResult;
    }

    private Future<BidResponse> cacheBidsAndCreateResponse(List<BidderResponseInfo> bidderResponses,
                                                           AuctionContext auctionContext,
                                                           BidRequestCacheInfo cacheInfo,
                                                           Map<String, MultiBidConfig> bidderToMultiBids,
                                                           VideoStoredDataResult videoStoredDataResult,
                                                           EventsContext eventsContext) {

        final BidRequest bidRequest = auctionContext.getBidRequest();
        if (isEmptyBidderResponses(bidderResponses)) {

            final ExtBidResponse extBidResponse = toExtBidResponse(
                    bidderResponses,
                    auctionContext,
                    CacheServiceResult.empty(),
                    VideoStoredDataResult.empty(),
                    eventsContext.getAuctionTimestamp(),
                    null,
                    null);
            final CachedDebugLog cachedDebugLog = auctionContext.getCachedDebugLog();
            if (isCachedDebugEnabled(cachedDebugLog)) {
                cachedDebugLog.setExtBidResponse(extBidResponse);
            }

            return Future.succeededFuture(BidResponse.builder()
                    .id(bidRequest.getId())
                    .cur(bidRequest.getCur().getFirst())
                    .nbr(0) // signal "Unknown Error"
                    .seatbid(Collections.emptyList())
                    .ext(extBidResponse)
                    .build());
        }

        final ExtRequestTargeting targeting = targeting(bidRequest);

        final List<BidderResponseInfo> bidderResponseInfos = toBidderResponseWithTargetingBidInfos(
                bidderResponses,
                bidderToMultiBids,
                preferDeals(targeting));

        final Set<BidInfo> bidInfos = bidderResponseInfos.stream()
                .map(BidderResponseInfo::getSeatBid)
                .map(BidderSeatBidInfo::getBidsInfos)
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        final Set<BidInfo> winningBidInfos = targeting == null
                ? null
                : bidInfos.stream()
                .filter(bidInfo -> bidInfo.getTargetingInfo().isWinningBid())
                .collect(Collectors.toSet());

        final Set<BidInfo> bidsToCache = cacheInfo.isShouldCacheWinningBidsOnly() ? winningBidInfos : bidInfos;

        return cacheBids(bidsToCache, auctionContext, cacheInfo, eventsContext)
                .map(cacheResult -> toBidResponse(
                        bidderResponseInfos,
                        auctionContext,
                        targeting,
                        cacheInfo,
                        cacheResult,
                        videoStoredDataResult,
                        eventsContext));
    }

    private static ExtRequestTargeting targeting(BidRequest bidRequest) {
        final ExtRequest ext = bidRequest.getExt();
        final ExtRequestPrebid prebid = ext != null ? ext.getPrebid() : null;
        return prebid != null ? prebid.getTargeting() : null;
    }

    private static boolean preferDeals(ExtRequestTargeting targeting) {
        return BooleanUtils.toBooleanDefaultIfNull(targeting != null ? targeting.getPreferdeals() : null, false);
    }

    private List<BidderResponseInfo> toBidderResponseWithTargetingBidInfos(
            List<BidderResponseInfo> bidderResponses,
            Map<String, MultiBidConfig> bidderToMultiBids,
            boolean preferDeals) {

        final Comparator<BidInfo> comparator = winningBidComparatorFactory.create(preferDeals).reversed();

        final List<List<BidInfo>> bidInfosPerBidder = bidderResponses.stream()
                .map(bidderResponse -> limitMultiBid(bidderResponse, bidderToMultiBids, comparator))
                .toList();
        final List<List<BidInfo>> rankedBidInfos = applyRanking(bidInfosPerBidder, comparator);

        return IntStream.range(0, bidderResponses.size())
                .mapToObj(i -> enrichBidInfoWithTargeting(
                        bidderResponses.get(i),
                        rankedBidInfos.get(i),
                        bidderToMultiBids))
                .toList();
    }

    private static List<BidInfo> limitMultiBid(BidderResponseInfo bidderResponse,
                                               Map<String, MultiBidConfig> bidderToMultiBids,
                                               Comparator<BidInfo> comparator) {

        final MultiBidConfig multiBid = bidderToMultiBids.get(bidderResponse.getBidder());
        final Integer bidLimit = multiBid != null ? multiBid.getMaxBids() : DEFAULT_BID_LIMIT_MIN;

        final List<BidInfo> bidInfos = bidderResponse.getSeatBid().getBidsInfos();
        final Map<String, List<BidInfo>> impIdToBidInfos = bidInfos.stream()
                .collect(Collectors.groupingBy(bidInfo -> bidInfo.getCorrespondingImp().getId()));

        return impIdToBidInfos.values().stream()
                .flatMap(infos -> infos.stream()
                        .sorted(comparator)
                        .limit(bidLimit))
                .toList();
    }

    private static List<List<BidInfo>> applyRanking(List<List<BidInfo>> bidInfosPerBidder,
                                                    Comparator<BidInfo> comparator) {

        final Map<String, List<Pair<Integer, BidInfo>>> impIdToBidderBidInfo = new HashMap<>();
        for (int bidderIndex = 0; bidderIndex < bidInfosPerBidder.size(); bidderIndex++) {
            final List<BidInfo> bidInfos = bidInfosPerBidder.get(bidderIndex);

            for (BidInfo bidInfo : bidInfos) {
                impIdToBidderBidInfo
                        .computeIfAbsent(bidInfo.getCorrespondingImp().getId(), ignore -> new ArrayList<>())
                        .add(Pair.of(bidderIndex, bidInfo));
            }
        }

        for (List<Pair<Integer, BidInfo>> bidderToBidInfo : impIdToBidderBidInfo.values()) {
            bidderToBidInfo.sort(Comparator.comparing(Pair::getRight, comparator));
        }

        final List<List<BidInfo>> rankedBidInfosPerBidder = new ArrayList<>();
        for (int i = 0; i < bidInfosPerBidder.size(); i++) {
            rankedBidInfosPerBidder.add(new ArrayList<>());
        }

        for (List<Pair<Integer, BidInfo>> sortedBidderToBidInfo : impIdToBidderBidInfo.values()) {
            for (int rank = 0; rank < sortedBidderToBidInfo.size(); rank++) {
                final Pair<Integer, BidInfo> bidderToBidInfo = sortedBidderToBidInfo.get(rank);
                final BidInfo bidInfo = bidderToBidInfo.getRight();

                rankedBidInfosPerBidder.get(bidderToBidInfo.getLeft())
                        .add(bidInfo.toBuilder().rank(rank + 1).build());
            }
        }

        return rankedBidInfosPerBidder;
    }

    private static BidderResponseInfo enrichBidInfoWithTargeting(BidderResponseInfo bidderResponseInfo,
                                                                 List<BidInfo> bidderBidInfos,
                                                                 Map<String, MultiBidConfig> bidderToMultiBids) {

        final String bidder = bidderResponseInfo.getBidder();
        final List<BidInfo> bidInfosWithTargeting = toBidInfoWithTargeting(bidderBidInfos, bidder, bidderToMultiBids);

        final BidderSeatBidInfo seatBid = bidderResponseInfo.getSeatBid();
        final BidderSeatBidInfo modifiedSeatBid = seatBid.with(bidInfosWithTargeting);
        return bidderResponseInfo.with(modifiedSeatBid);
    }

    private static List<BidInfo> toBidInfoWithTargeting(List<BidInfo> bidderBidInfos,
                                                        String bidder,
                                                        Map<String, MultiBidConfig> bidderToMultiBids) {

        final Map<String, List<BidInfo>> impIdToBidInfos = bidderBidInfos.stream()
                .collect(Collectors.groupingBy(bidInfo -> bidInfo.getCorrespondingImp().getId()));

        return impIdToBidInfos.values().stream()
                .map(bidInfos -> enrichWithTargeting(bidInfos, bidder, bidderToMultiBids))
                .flatMap(Collection::stream)
                .toList();
    }

    private static List<BidInfo> enrichWithTargeting(List<BidInfo> bidderImpIdBidInfos,
                                                     String bidder,
                                                     Map<String, MultiBidConfig> bidderToMultiBids) {

        final List<BidInfo> result = new ArrayList<>();

        final MultiBidConfig multiBid = bidderToMultiBids.get(bidder);
        final String bidderCodePrefix = multiBid != null ? multiBid.getTargetBidderCodePrefix() : null;

        final int multiBidSize = bidderImpIdBidInfos.size();
        for (int i = 0; i < multiBidSize; i++) {
            // first bid have the highest value and can't be extra bid
            final String targetingBidderCode = targetingCode(bidder, bidderCodePrefix, i);
            final BidInfo bidInfo = bidderImpIdBidInfos.get(i);

            final TargetingInfo targetingInfo = TargetingInfo.builder()
                    .isTargetingEnabled(targetingBidderCode != null)
                    .isWinningBid(bidInfo.getRank() == 1)
                    .isAddTargetBidderCode(targetingBidderCode != null && multiBidSize > 1)
                    .bidderCode(targetingBidderCode)
                    .seat(targetingCode(bidInfo.getSeat(), bidderCodePrefix, i))
                    .build();

            final BidInfo modifiedBidInfo = bidInfo.toBuilder().targetingInfo(targetingInfo).build();
            result.add(modifiedBidInfo);
        }

        return result;
    }

    private static String targetingCode(String base, String prefix, int i) {
        if (i == 0) {
            return base;
        }

        return prefix != null ? prefix + (i + 1) : null;
    }

    private ExtBidResponse toExtBidResponse(List<BidderResponseInfo> bidderResponseInfos,
                                            AuctionContext auctionContext,
                                            CacheServiceResult cacheResult,
                                            VideoStoredDataResult videoStoredDataResult,
                                            long auctionTimestamp,
                                            Map<String, List<ExtBidderError>> bidErrors,
                                            Map<String, List<ExtBidderError>> bidWarnings) {

        final DebugContext debugContext = auctionContext.getDebugContext();
        final boolean debugEnabled = debugContext.isDebugEnabled();

        final PaaResult paaResult = toPaaOutput(bidderResponseInfos, auctionContext);
        final List<ExtIgi> igi = paaResult.igis();
        final ExtBidResponseFledge fledge = paaResult.fledge();

        final ExtResponseDebug extResponseDebug = toExtResponseDebug(
                bidderResponseInfos, auctionContext, cacheResult, debugEnabled);
        final Map<String, List<ExtBidderError>> errors = toExtBidderErrors(
                bidderResponseInfos, auctionContext, cacheResult, videoStoredDataResult, bidErrors);
        final Map<String, List<ExtBidderError>> warnings = toExtBidderWarnings(
                bidderResponseInfos, auctionContext, bidWarnings);

        final Map<String, Integer> responseTimeMillis = toResponseTimes(bidderResponseInfos, cacheResult);

        final ExtBidResponsePrebid prebid = toExtBidResponsePrebid(
                auctionTimestamp, auctionContext.getBidRequest(), fledge);

        return ExtBidResponse.builder()
                .debug(extResponseDebug)
                .errors(errors)
                .warnings(warnings)
                .responsetimemillis(responseTimeMillis)
                .tmaxrequest(auctionContext.getBidRequest().getTmax())
                .igi(igi)
                .prebid(prebid)
                .build();
    }

    private ExtBidResponsePrebid toExtBidResponsePrebid(long auctionTimestamp,
                                                        BidRequest bidRequest,
                                                        ExtBidResponseFledge extBidResponseFledge) {

        final JsonNode passThrough = Optional.ofNullable(bidRequest)
                .map(BidRequest::getExt)
                .map(ExtRequest::getPrebid)
                .map(ExtRequestPrebid::getPassthrough)
                .orElse(null);

        return ExtBidResponsePrebid.builder()
                .auctiontimestamp(auctionTimestamp)
                .passthrough(passThrough)
                .fledge(extBidResponseFledge)
                .build();
    }

    private PaaResult toPaaOutput(List<BidderResponseInfo> bidderResponseInfos, AuctionContext auctionContext) {

        final PaaFormat paaFormat = resolvePaaFormat(auctionContext);
        final List<ExtIgi> igis = extractIgis(bidderResponseInfos, auctionContext);
        final List<ExtIgi> extIgi = paaFormat == PaaFormat.IAB && !igis.isEmpty() ? igis : null;

        final List<FledgeAuctionConfig> fledgeConfigs = paaFormat == PaaFormat.ORIGINAL
                ? toOriginalFledgeFormat(igis)
                : Collections.emptyList();

        // TODO: Remove after transition period
        final List<Imp> imps = auctionContext.getBidRequest().getImp();
        final List<FledgeAuctionConfig> deprecatedFledgeConfigs = bidderResponseInfos.stream()
                .flatMap(bidderResponseInfo -> toDeprecatedFledgeConfigs(bidderResponseInfo, imps))
                .toList();

        final List<FledgeAuctionConfig> combinedFledgeConfigs = ListUtils.union(deprecatedFledgeConfigs, fledgeConfigs);
        final ExtBidResponseFledge extBidResponseFledge = combinedFledgeConfigs.isEmpty()
                ? null
                : ExtBidResponseFledge.of(combinedFledgeConfigs);

        return new PaaResult(extIgi, extBidResponseFledge);
    }

    private List<ExtIgi> extractIgis(List<BidderResponseInfo> bidderResponseInfos, AuctionContext auctionContext) {
        return bidderResponseInfos.stream()
                .flatMap(responseInfo -> responseInfo.getSeatBid().getIgi().stream()
                        .map(igi -> prepareExtIgi(
                                igi,
                                responseInfo.getSeat(),
                                responseInfo.getAdapterCode(),
                                auctionContext)))
                .filter(Objects::nonNull)
                .toList();
    }

    private ExtIgi prepareExtIgi(ExtIgi igi,
                                 String seat,
                                 String adapterCode,
                                 AuctionContext auctionContext) {
        if (igi == null) {
            return null;
        }

        final boolean shouldDropIgb = StringUtils.isEmpty(igi.getImpid()) && CollectionUtils.isNotEmpty(igi.getIgb());
        if (shouldDropIgb) {
            final String warning = "ExtIgi with absent impId from bidder: " + seat;
            if (auctionContext.getDebugContext().isDebugEnabled()) {
                auctionContext.getDebugWarnings().add(warning);
            }
            conditionalLogger.warn(warning, logSamplingRate);
            metrics.updateAlertsMetrics(MetricName.general);
        }

        final List<ExtIgiIgs> updatedIgs = prepareExtIgiIgs(igi.getIgs(), seat, adapterCode, auctionContext);
        final List<ExtIgiIgs> preparedIgs = updatedIgs.isEmpty() ? null : updatedIgs;
        final List<ExtIgiIgb> preparedIgb = shouldDropIgb ? null : igi.getIgb();

        return ObjectUtils.anyNotNull(preparedIgs, preparedIgb)
                ? igi.toBuilder().igs(preparedIgs).igb(preparedIgb).build()
                : null;
    }

    private List<ExtIgiIgs> prepareExtIgiIgs(List<ExtIgiIgs> igiIgs,
                                             String seat,
                                             String adapterCode,
                                             AuctionContext auctionContext) {

        if (igiIgs == null) {
            return Collections.emptyList();
        }

        final boolean debugEnabled = auctionContext.getDebugContext().isDebugEnabled();
        final List<ExtIgiIgs> preparedIgiIgs = new ArrayList<>();
        for (ExtIgiIgs extIgiIgs : igiIgs) {
            if (extIgiIgs == null) {
                continue;
            }

            if (StringUtils.isEmpty(extIgiIgs.getImpId())) {
                final String warning = "ExtIgiIgs with absent impId from bidder: " + seat;
                if (debugEnabled) {
                    auctionContext.getDebugWarnings().add(warning);
                }
                conditionalLogger.warn(warning, logSamplingRate);
                metrics.updateAlertsMetrics(MetricName.general);
                continue;
            }

            if (extIgiIgs.getConfig() == null) {
                final String warning = "ExtIgiIgs with absent config from bidder: " + seat;
                if (debugEnabled) {
                    auctionContext.getDebugWarnings().add(warning);
                }
                conditionalLogger.warn(warning, logSamplingRate);
                metrics.updateAlertsMetrics(MetricName.general);
                continue;
            }

            final ExtIgiIgs preparedExtIgiIgs = extIgiIgs.toBuilder()
                    .ext(ExtIgiIgsExt.of(seat, adapterCode))
                    .build();

            preparedIgiIgs.add(preparedExtIgiIgs);
        }

        return preparedIgiIgs;
    }

    private List<FledgeAuctionConfig> toOriginalFledgeFormat(List<ExtIgi> igis) {
        return igis.stream()
                .map(ExtIgi::getIgs)
                .flatMap(Collection::stream)
                .map(BidResponseCreator::extIgiIgsToFledgeConfig)
                .toList();
    }

    private static FledgeAuctionConfig extIgiIgsToFledgeConfig(ExtIgiIgs extIgiIgs) {
        return FledgeAuctionConfig.builder()
                .bidder(extIgiIgs.getExt().getBidder())
                .adapter(extIgiIgs.getExt().getAdapter())
                .impId(extIgiIgs.getImpId())
                .config(extIgiIgs.getConfig())
                .build();
    }

    private Stream<FledgeAuctionConfig> toDeprecatedFledgeConfigs(BidderResponseInfo bidderResponseInfo,
                                                                  List<Imp> imps) {

        return Optional.ofNullable(bidderResponseInfo.getSeatBid().getFledgeAuctionConfigs())
                .stream()
                .flatMap(Collection::stream)
                .filter(fledgeConfig -> validateFledgeConfig(fledgeConfig, imps))
                .map(fledgeConfig -> fledgeConfigWithBidder(
                        fledgeConfig,
                        bidderResponseInfo.getSeat(),
                        bidderResponseInfo.getAdapterCode()));
    }

    private boolean validateFledgeConfig(FledgeAuctionConfig fledgeAuctionConfig, List<Imp> imps) {
        final ExtImpAuctionEnvironment fledgeEnabled = correspondingImp(fledgeAuctionConfig.getImpId(), imps)
                .map(Imp::getExt)
                .map(ext -> convertValue(ext, "ae", ExtImpAuctionEnvironment.class))
                .orElse(ExtImpAuctionEnvironment.SERVER_SIDE_AUCTION);

        return fledgeEnabled == ExtImpAuctionEnvironment.ON_DEVICE_IG_AUCTION_FLEDGE;
    }

    private FledgeAuctionConfig fledgeConfigWithBidder(FledgeAuctionConfig fledgeConfig,
                                                       String seat,
                                                       String adapterCode) {

        return fledgeConfig.toBuilder()
                .bidder(seat)
                .adapter(adapterCode)
                .build();
    }

    private static ExtResponseDebug toExtResponseDebug(List<BidderResponseInfo> bidderResponseInfos,
                                                       AuctionContext auctionContext,
                                                       CacheServiceResult cacheResult,
                                                       boolean debugEnabled) {

        final Map<String, List<ExtHttpCall>> httpCalls = debugEnabled
                ? toExtHttpCalls(bidderResponseInfos, cacheResult, auctionContext.getDebugHttpCalls())
                : null;

        final BidRequest bidRequest = debugEnabled ? auctionContext.getBidRequest() : null;
        final ExtDebugTrace extDebugTrace = toExtDebugTrace(auctionContext);

        return ObjectUtils.anyNotNull(httpCalls, bidRequest, extDebugTrace)
                ? ExtResponseDebug.of(httpCalls, bidRequest, extDebugTrace)
                : null;
    }

    /**
     * Corresponds cacheId (or null if not present) to each {@link Bid}.
     */
    private Future<CacheServiceResult> cacheBids(Set<BidInfo> bidsToCache,
                                                 AuctionContext auctionContext,
                                                 BidRequestCacheInfo cacheInfo,
                                                 EventsContext eventsContext) {

        if (!cacheInfo.isDoCaching()) {
            return Future.succeededFuture(CacheServiceResult.of(null, null, toMapBidsWithEmptyCacheIds(bidsToCache)));
        }

        // do not submit non deals bids with zero price to prebid cache
        final List<BidInfo> bidsValidToBeCached = bidsToCache.stream()
                .filter(BidResponseCreator::isValidForCaching)
                .toList();

        final CacheContext cacheContext = CacheContext.builder()
                .shouldCacheBids(cacheInfo.isShouldCacheBids())
                .shouldCacheVideoBids(cacheInfo.isShouldCacheVideoBids())
                .build();

        return coreCacheService.cacheBidsOpenrtb(bidsValidToBeCached, auctionContext, cacheContext, eventsContext)
                .map(cacheResult -> addNotCachedBids(cacheResult, bidsToCache));
    }

    private static boolean isValidForCaching(BidInfo bidInfo) {
        final Bid bid = bidInfo.getBid();
        final BigDecimal price = bid.getPrice();
        return bid.getDealid() != null ? price.compareTo(BigDecimal.ZERO) >= 0 : price.compareTo(BigDecimal.ZERO) > 0;
    }

    /**
     * Creates a map with {@link Bid} as a key and null as a value.
     */
    private static Map<Bid, CacheInfo> toMapBidsWithEmptyCacheIds(Set<BidInfo> bids) {
        return bids.stream()
                .map(BidInfo::getBid)
                .collect(Collectors.toMap(Function.identity(), ignored -> CacheInfo.empty()));
    }

    /**
     * Adds bids with no cache id info.
     */
    private static CacheServiceResult addNotCachedBids(CacheServiceResult cacheResult, Set<BidInfo> bidInfos) {
        final Map<Bid, CacheInfo> bidToCacheId = cacheResult.getCacheBids();

        if (bidInfos.size() > bidToCacheId.size()) {
            final Map<Bid, CacheInfo> updatedBidToCacheInfo = new HashMap<>(bidToCacheId);
            for (BidInfo bidInfo : bidInfos) {
                final Bid bid = bidInfo.getBid();
                if (!updatedBidToCacheInfo.containsKey(bid)) {
                    updatedBidToCacheInfo.put(bid, CacheInfo.empty());
                }
            }
            return CacheServiceResult.of(cacheResult.getHttpCall(), cacheResult.getError(), updatedBidToCacheInfo);
        }
        return cacheResult;
    }

    private static Map<String, List<ExtHttpCall>> toExtHttpCalls(List<BidderResponseInfo> bidderResponses,
                                                                 CacheServiceResult cacheResult,
                                                                 Map<String, List<DebugHttpCall>> contextHttpCalls) {

        final Map<String, List<ExtHttpCall>> bidderHttpCalls = bidderResponses.stream()
                .filter(bidderResponse -> CollectionUtils.isNotEmpty(bidderResponse.getSeatBid().getHttpCalls()))
                .collect(Collectors.toMap(
                        BidderResponseInfo::getSeat,
                        bidderResponse -> bidderResponse.getSeatBid().getHttpCalls(),
                        ListUtil::union));

        final DebugHttpCall httpCall = cacheResult.getHttpCall();
        final ExtHttpCall cacheExtHttpCall = httpCall != null ? toExtHttpCall(httpCall) : null;
        final Map<String, List<ExtHttpCall>> cacheHttpCalls = cacheExtHttpCall != null
                ? Collections.singletonMap(CACHE, Collections.singletonList(cacheExtHttpCall))
                : Collections.emptyMap();

        final Map<String, List<ExtHttpCall>> contextExtHttpCalls = contextHttpCalls.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, serviceToHttpCall -> serviceToHttpCall.getValue().stream()
                        .map(BidResponseCreator::toExtHttpCall)
                        .toList()));

        final Map<String, List<ExtHttpCall>> httpCalls = new HashMap<>();
        httpCalls.putAll(bidderHttpCalls);
        httpCalls.putAll(cacheHttpCalls);
        httpCalls.putAll(contextExtHttpCalls);
        return httpCalls.isEmpty() ? null : httpCalls;
    }

    private static ExtHttpCall toExtHttpCall(DebugHttpCall debugHttpCall) {
        return ExtHttpCall.builder()
                .uri(debugHttpCall.getRequestUri())
                .requestbody(debugHttpCall.getRequestBody())
                .status(debugHttpCall.getResponseStatus())
                .responsebody(debugHttpCall.getResponseBody())
                .requestheaders(debugHttpCall.getRequestHeaders())
                .build();
    }

    private static ExtDebugTrace toExtDebugTrace(AuctionContext auctionContext) {
        if (auctionContext.getDebugContext().getTraceLevel() == null) {
            return null;
        }

        final List<ExtTraceActivityInfrastructure> activityInfrastructureTrace =
                new ArrayList<>(auctionContext.getActivityInfrastructure().debugTrace());

        return CollectionUtils.isNotEmpty(activityInfrastructureTrace)
                ? ExtDebugTrace.of(activityInfrastructureTrace)
                : null;
    }

    private Map<String, List<ExtBidderError>> toExtBidderErrors(List<BidderResponseInfo> bidderResponses,
                                                                AuctionContext auctionContext,
                                                                CacheServiceResult cacheResult,
                                                                VideoStoredDataResult videoStoredDataResult,
                                                                Map<String, List<ExtBidderError>> bidErrors) {

        final Map<String, List<ExtBidderError>> errors = new HashMap<>();
        errors.putAll(extractBidderErrors(bidderResponses));
        errors.putAll(extractDeprecatedBiddersErrors(auctionContext.getBidRequest()));
        errors.putAll(extractPrebidErrors(videoStoredDataResult, auctionContext));
        errors.putAll(extractCacheErrors(cacheResult));
        if (MapUtils.isNotEmpty(bidErrors)) {
            addBidErrors(errors, bidErrors);
        }
        return errors.isEmpty() ? null : errors;
    }

    /**
     * Returns a map with bidder name as a key and list of {@link ExtBidderError}s as a value.
     */
    private static Map<String, List<ExtBidderError>> extractBidderErrors(
            Collection<BidderResponseInfo> bidderResponses) {

        return bidderResponses.stream()
                .filter(bidderResponse -> CollectionUtils.isNotEmpty(bidderResponse.getSeatBid().getErrors()))
                .collect(Collectors.toMap(
                        BidderResponseInfo::getSeat,
                        bidderResponse -> errorsDetails(bidderResponse.getSeatBid().getErrors()),
                        ListUtil::union));
    }

    /**
     * Returns a map with bidder name as a key and list of {@link ExtBidderError}s as a value.
     */
    private static Map<String, List<ExtBidderError>> extractBidderWarnings(
            Collection<BidderResponseInfo> bidderResponses) {

        return bidderResponses.stream()
                .filter(bidderResponse -> CollectionUtils.isNotEmpty(bidderResponse.getSeatBid().getWarnings()))
                .collect(Collectors.toMap(
                        BidderResponseInfo::getSeat,
                        bidderResponse -> errorsDetails(bidderResponse.getSeatBid().getWarnings()),
                        ListUtil::union));
    }

    /**
     * Maps a list of {@link BidderError} to a list of {@link ExtBidderError}s.
     */
    private static List<ExtBidderError> errorsDetails(List<BidderError> errors) {
        return errors.stream()
                .map(bidderError -> ExtBidderError.of(
                        bidderError.getType().getCode(),
                        bidderError.getMessage(),
                        nullIfEmpty(bidderError.getImpIds())))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Returns a map with deprecated bidder name as a key and list of {@link ExtBidderError}s as a value.
     */
    private Map<String, List<ExtBidderError>> extractDeprecatedBiddersErrors(BidRequest bidRequest) {
        return bidRequest.getImp().stream()
                .flatMap(imp -> Optional.ofNullable(imp.getExt())
                        .flatMap(ext -> getExtPrebid(ext, ExtImpPrebid.class))
                        .map(ExtImpPrebid::getBidder)
                        .map(ObjectNode::fieldNames)
                        .map(StreamUtil::asStream)
                        .orElseGet(Stream::empty)
                )
                .distinct()
                .filter(bidderCatalog::isDeprecatedName)
                .collect(Collectors.toMap(Function.identity(),
                        bidder -> Collections.singletonList(ExtBidderError.of(BidderError.Type.bad_input.getCode(),
                                bidderCatalog.errorForDeprecatedName(bidder)))));
    }

    /**
     * Returns a singleton map with "prebid" as a key and list of {@link ExtBidderError}s errors as a value.
     */
    private static Map<String, List<ExtBidderError>> extractPrebidErrors(VideoStoredDataResult videoStoredDataResult,
                                                                         AuctionContext auctionContext) {

        final List<ExtBidderError> storedErrors = extractStoredErrors(videoStoredDataResult);
        final List<ExtBidderError> contextErrors = extractContextErrors(auctionContext);
        if (storedErrors.isEmpty() && contextErrors.isEmpty()) {
            return Collections.emptyMap();
        }

        final List<ExtBidderError> collectedErrors =
                Stream.concat(contextErrors.stream(), storedErrors.stream()).toList();
        return Collections.singletonMap(DEFAULT_DEBUG_KEY, collectedErrors);
    }

    /**
     * Returns a list of {@link ExtBidderError}s of stored request errors.
     */
    private static List<ExtBidderError> extractStoredErrors(VideoStoredDataResult videoStoredDataResult) {
        final List<String> errors = videoStoredDataResult.getErrors();
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors.stream()
                    .map(message -> ExtBidderError.of(BidderError.Type.generic.getCode(), message))
                    .toList();
        }
        return Collections.emptyList();
    }

    /**
     * Returns a list of {@link ExtBidderError}s of auction context prebid errors.
     */
    private static List<ExtBidderError> extractContextErrors(AuctionContext auctionContext) {
        return auctionContext.getPrebidErrors().stream()
                .map(message -> ExtBidderError.of(BidderError.Type.generic.getCode(), message))
                .toList();
    }

    /**
     * Returns a singleton map with "cache" as a key and list of {@link ExtBidderError}s cache errors as a value.
     */
    private static Map<String, List<ExtBidderError>> extractCacheErrors(CacheServiceResult cacheResult) {
        final Throwable error = cacheResult.getError();
        if (error != null) {
            final ExtBidderError extBidderError = ExtBidderError.of(BidderError.Type.generic.getCode(),
                    error.getMessage());
            return Collections.singletonMap(CACHE, Collections.singletonList(extBidderError));
        }
        return Collections.emptyMap();
    }

    /**
     * Adds bid errors: if value by key exists - add errors to its list, otherwise - add an entry.
     */
    private static void addBidErrors(Map<String, List<ExtBidderError>> errors,
                                     Map<String, List<ExtBidderError>> bidErrors) {

        for (Map.Entry<String, List<ExtBidderError>> errorEntry : bidErrors.entrySet()) {
            final List<ExtBidderError> extBidderErrors = errors.get(errorEntry.getKey());
            if (extBidderErrors != null) {
                extBidderErrors.addAll(errorEntry.getValue());
            } else {
                errors.put(errorEntry.getKey(), errorEntry.getValue());
            }
        }
    }

    private static Map<String, List<ExtBidderError>> toExtBidderWarnings(
            List<BidderResponseInfo> bidderResponses,
            AuctionContext auctionContext,
            Map<String, List<ExtBidderError>> bidWarnings) {

        final Map<String, List<ExtBidderError>> warnings = new HashMap<>();

        warnings.putAll(extractContextWarnings(auctionContext));
        warnings.putAll(extractBidderWarnings(bidderResponses));
        warnings.putAll(MapUtils.isEmpty(bidWarnings) ? Collections.emptyMap() : bidWarnings);

        return warnings.isEmpty() ? null : warnings;
    }

    private static Map<String, List<ExtBidderError>> extractContextWarnings(AuctionContext auctionContext) {
        final List<ExtBidderError> contextWarnings = auctionContext.getDebugWarnings().stream()
                .map(message -> ExtBidderError.of(BidderError.Type.generic.getCode(), message))
                .toList();

        return contextWarnings.isEmpty()
                ? Collections.emptyMap()
                : Collections.singletonMap(DEFAULT_DEBUG_KEY, contextWarnings);
    }

    /**
     * Returns a map with response time by bidders and cache.
     */
    private static Map<String, Integer> toResponseTimes(Collection<BidderResponseInfo> bidderResponses,
                                                        CacheServiceResult cacheResult) {

        final Map<String, Integer> responseTimeMillis = bidderResponses.stream()
                .collect(Collectors.toMap(BidderResponseInfo::getSeat, BidderResponseInfo::getResponseTime, Math::max));

        final DebugHttpCall debugHttpCall = cacheResult.getHttpCall();
        final Integer cacheResponseTime = debugHttpCall != null ? debugHttpCall.getResponseTimeMillis() : null;
        if (cacheResponseTime != null) {
            responseTimeMillis.put(CACHE, cacheResponseTime);
        }
        return responseTimeMillis;
    }

    private static PaaFormat resolvePaaFormat(AuctionContext auctionContext) {
        return Optional.of(auctionContext.getBidRequest())
                .map(BidRequest::getExt)
                .map(ExtRequest::getPrebid)
                .map(ExtRequestPrebid::getPaaFormat)
                .or(() -> Optional.ofNullable(auctionContext.getAccount())
                        .map(Account::getAuction)
                        .map(AccountAuctionConfig::getPaaFormat))
                .orElse(PaaFormat.ORIGINAL);
    }

    /**
     * Returns {@link BidResponse} based on list of {@link BidderResponse}s and {@link CacheServiceResult}.
     */
    private BidResponse toBidResponse(List<BidderResponseInfo> bidderResponseInfos,
                                      AuctionContext auctionContext,
                                      ExtRequestTargeting targeting,
                                      BidRequestCacheInfo requestCacheInfo,
                                      CacheServiceResult cacheResult,
                                      VideoStoredDataResult videoStoredDataResult,
                                      EventsContext eventsContext) {

        final BidRequest bidRequest = auctionContext.getBidRequest();
        final Account account = auctionContext.getAccount();

        final Map<String, List<ExtBidderError>> bidErrors = new HashMap<>();
        final Map<String, List<ExtBidderError>> bidWarnings = new HashMap<>();
        final List<SeatBid> seatBids = bidderResponseInfos.stream()
                .map(BidderResponseInfo::getSeatBid)
                .map(BidderSeatBidInfo::getBidsInfos)
                .filter(CollectionUtils::isNotEmpty)
                .map(bidInfos -> toSeatBid(
                        bidInfos,
                        targeting,
                        bidRequest,
                        requestCacheInfo,
                        cacheResult.getCacheBids(),
                        account,
                        bidErrors,
                        bidWarnings))
                .toList();

        final Long auctionTimestamp = eventsContext.getAuctionTimestamp();
        final ExtBidResponse extBidResponse = toExtBidResponse(
                bidderResponseInfos,
                auctionContext,
                cacheResult,
                videoStoredDataResult,
                auctionTimestamp,
                bidErrors,
                bidWarnings);

        final CachedDebugLog cachedDebugLog = auctionContext.getCachedDebugLog();
        if (isCachedDebugEnabled(cachedDebugLog)) {
            cachedDebugLog.setExtBidResponse(extBidResponse);
        }

        return BidResponse.builder()
                .id(bidRequest.getId())
                .cur(bidRequest.getCur().getFirst())
                .seatbid(seatBids)
                .ext(extBidResponse)
                .build();
    }

    private Future<VideoStoredDataResult> videoStoredDataResult(AuctionContext auctionContext) {
        final List<Imp> imps = auctionContext.getBidRequest().getImp();
        final String accountId = auctionContext.getAccount().getId();
        final Timeout timeout = auctionContext.getTimeoutContext().getTimeout();

        final List<String> errors = new ArrayList<>();
        final List<Imp> storedVideoInjectableImps = new ArrayList<>();

        for (Imp imp : imps) {
            try {
                if (checkEchoVideoAttrs(imp)) {
                    storedVideoInjectableImps.add(imp);
                }
            } catch (InvalidRequestException e) {
                errors.add(e.getMessage());
            }
        }

        return storedRequestProcessor.videoStoredDataResult(accountId, storedVideoInjectableImps, errors, timeout)
                .otherwise(throwable -> VideoStoredDataResult.of(Collections.emptyMap(),
                        Collections.singletonList(throwable.getMessage())));
    }

    /**
     * Checks if imp.ext.prebid.options.echovideoattrs equals true.
     */
    private boolean checkEchoVideoAttrs(Imp imp) {
        if (imp.getExt() != null) {
            try {
                final ExtImp extImp = mapper.mapper().treeToValue(imp.getExt(), ExtImp.class);
                final ExtImpPrebid prebid = extImp.getPrebid();
                final ExtOptions options = prebid != null ? prebid.getOptions() : null;
                final Boolean echoVideoAttrs = options != null ? options.getEchoVideoAttrs() : null;
                return BooleanUtils.toBoolean(echoVideoAttrs);
            } catch (JsonProcessingException e) {
                throw new InvalidRequestException(
                        "Incorrect Imp extension format for Imp with id " + imp.getId() + ": " + e.getMessage());
            }
        }
        return false;
    }

    /**
     * Creates an OpenRTB {@link SeatBid} for a bidder. It will contain all the bids supplied by a bidder and a "bidder"
     * extension field populated.
     */
    private SeatBid toSeatBid(List<BidInfo> bidInfos,
                              ExtRequestTargeting targeting,
                              BidRequest bidRequest,
                              BidRequestCacheInfo requestCacheInfo,
                              Map<Bid, CacheInfo> bidToCacheInfo,
                              Account account,
                              Map<String, List<ExtBidderError>> bidErrors,
                              Map<String, List<ExtBidderError>> bidWarnings) {

        final List<Bid> bids = bidInfos.stream()
                .map(bidInfo -> injectAdmWithCacheInfo(
                        bidInfo,
                        requestCacheInfo,
                        bidToCacheInfo,
                        bidErrors))
                .filter(Objects::nonNull)
                .map(bidInfo -> toBid(
                        bidInfo,
                        targeting,
                        bidRequest,
                        account,
                        bidWarnings))
                .filter(Objects::nonNull)
                .toList();

        final String seat = bidInfos.stream()
                .map(BidInfo::getSeat)
                .findFirst()
                // Should never occur
                .orElseThrow(() -> new IllegalArgumentException("BidderCode was not defined for bidInfo"));

        return SeatBid.builder()
                .seat(seat)
                .bid(bids)
                .group(0) // prebid cannot support roadblocking
                .build();
    }

    private BidInfo injectAdmWithCacheInfo(BidInfo bidInfo,
                                           BidRequestCacheInfo requestCacheInfo,
                                           Map<Bid, CacheInfo> bidsWithCacheIds,
                                           Map<String, List<ExtBidderError>> bidErrors) {

        final Bid bid = bidInfo.getBid();
        final BidType bidType = bidInfo.getBidType();
        final String seat = bidInfo.getSeat();
        final Imp correspondingImp = bidInfo.getCorrespondingImp();

        final CacheInfo cacheInfo = bidsWithCacheIds.get(bid);
        final String cacheId = cacheInfo != null ? cacheInfo.getCacheId() : null;
        final String videoCacheId = cacheInfo != null ? cacheInfo.getVideoCacheId() : null;

        String modifiedBidAdm = bid.getAdm();
        if ((videoCacheId != null && !requestCacheInfo.isReturnCreativeVideoBids())
                || (cacheId != null && !requestCacheInfo.isReturnCreativeBids())) {
            modifiedBidAdm = null;
        }

        if (bidType.equals(BidType.xNative) && modifiedBidAdm != null) {
            try {
                modifiedBidAdm = createNativeMarkup(modifiedBidAdm, correspondingImp);
            } catch (PreBidException e) {
                bidErrors.computeIfAbsent(seat, ignored -> new ArrayList<>())
                        .add(ExtBidderError.of(BidderError.Type.bad_server_response.getCode(), e.getMessage()));
                return null;
            }
        }

        final Bid modifiedBid = bid.toBuilder().adm(modifiedBidAdm).build();
        return bidInfo.toBuilder()
                .bid(modifiedBid)
                .cacheInfo(cacheInfo)
                .build();
    }

    /**
     * Returns an OpenRTB {@link Bid} with "prebid" and "bidder" extension fields populated.
     */
    private Bid toBid(BidInfo bidInfo,
                      ExtRequestTargeting targeting,
                      BidRequest bidRequest,
                      Account account,
                      Map<String, List<ExtBidderError>> bidWarnings) {

        final TargetingInfo targetingInfo = bidInfo.getTargetingInfo();
        final BidType bidType = bidInfo.getBidType();
        final Bid bid = bidInfo.getBid();

        final CacheInfo cacheInfo = bidInfo.getCacheInfo();
        final String cacheId = cacheInfo != null ? cacheInfo.getCacheId() : null;
        final String videoCacheId = cacheInfo != null ? cacheInfo.getVideoCacheId() : null;

        final Map<String, String> targetingKeywords;
        if (shouldIncludeTargetingInResponse(targeting, bidInfo.getTargetingInfo())) {
            final TargetingKeywordsCreator keywordsCreator = resolveKeywordsCreator(
                    bidType, targeting, bidRequest, account, bidWarnings);

            final boolean isWinningBid = targetingInfo.isWinningBid();
            final String seat = targetingInfo.getSeat();
            final String categoryDuration = bidInfo.getCategory();
            targetingKeywords = keywordsCreator != null
                    ? keywordsCreator.makeFor(
                    bid, seat, isWinningBid, cacheId, bidType.getName(), videoCacheId, categoryDuration, account)
                    : null;
        } else {
            targetingKeywords = null;
        }

        final CacheAsset bids = cacheId != null ? toCacheAsset(cacheId) : null;
        final CacheAsset vastXml = videoCacheId != null ? toCacheAsset(videoCacheId) : null;
        final ExtResponseCache cache = bids != null || vastXml != null ? ExtResponseCache.of(bids, vastXml) : null;

        final ObjectNode originalBidExt = bid.getExt();
        final Boolean dealsTierSatisfied = bidInfo.getSatisfiedPriority();

        final boolean bidRankingEnabled = isBidRankingEnabled(account);

        final ExtBidPrebid updatedExtBidPrebid =
                getExtPrebid(originalBidExt, ExtBidPrebid.class)
                        .map(ExtBidPrebid::toBuilder)
                        .orElseGet(ExtBidPrebid::builder)
                        .targeting(MapUtils.isNotEmpty(targetingKeywords) ? targetingKeywords : null)
                        .targetBidderCode(targetingInfo.isAddTargetBidderCode() ? targetingInfo.getBidderCode() : null)
                        .dealTierSatisfied(dealsTierSatisfied)
                        .cache(cache)
                        .passThrough(extractPassThrough(bidInfo.getCorrespondingImp()))
                        .rank(bidRankingEnabled ? bidInfo.getRank() : null)
                        .build();

        final ObjectNode updatedBidExt =
                originalBidExt != null ? originalBidExt.deepCopy() : mapper.mapper().createObjectNode();
        updatedBidExt.set(PREBID_EXT, mapper.mapper().valueToTree(updatedExtBidPrebid));
        final Integer ttl = Optional.ofNullable(cacheInfo)
                .map(info -> ObjectUtils.max(cacheInfo.getTtl(), cacheInfo.getVideoTtl()))
                .orElseGet(() -> ObjectUtils.max(bidInfo.getTtl(), bidInfo.getVastTtl()));
        return bid.toBuilder()
                .ext(updatedBidExt)
                .exp(ttl)
                .build();
    }

    private boolean shouldIncludeTargetingInResponse(ExtRequestTargeting targeting, TargetingInfo targetingInfo) {
        return targeting != null
                && targetingInfo.isTargetingEnabled()
                && (Objects.equals(targeting.getIncludebidderkeys(), true)
                || Objects.equals(targeting.getIncludewinners(), true)
                || Objects.equals(targeting.getIncludeformat(), true));
    }

    private JsonNode extractPassThrough(Imp imp) {
        return Optional.ofNullable(imp.getExt())
                .flatMap(ext -> getExtPrebid(ext, ExtImpPrebid.class))
                .map(ExtImpPrebid::getPassthrough)
                .orElse(null);
    }

    private static boolean isBidRankingEnabled(Account account) {
        return Optional.ofNullable(account.getAuction())
                .map(AccountAuctionConfig::getRanking)
                .map(AccountBidRankingConfig::getEnabled)
                .orElse(false);
    }

    private String createNativeMarkup(String bidAdm, Imp correspondingImp) {
        final Response nativeMarkup;
        try {
            nativeMarkup = mapper.decodeValue(bidAdm, Response.class);
        } catch (DecodeException e) {
            throw new PreBidException(e.getMessage());
        }

        final List<Asset> responseAssets = nativeMarkup.getAssets();
        if (CollectionUtils.isNotEmpty(responseAssets)) {
            final Native nativeImp = correspondingImp != null ? correspondingImp.getXNative() : null;
            if (nativeImp == null) {
                throw new PreBidException("Could not find native imp");
            }

            final Request nativeRequest;
            try {
                nativeRequest = mapper.mapper().readValue(nativeImp.getRequest(), Request.class);
            } catch (JsonProcessingException e) {
                throw new PreBidException(e.getMessage());
            }

            responseAssets.forEach(asset -> setAssetTypes(asset, nativeRequest.getAssets()));
            return mapper.encodeToString(nativeMarkup);
        }

        return bidAdm;
    }

    private static void setAssetTypes(Asset responseAsset, List<com.iab.openrtb.request.Asset> requestAssets) {
        if (responseAsset.getImg() != null) {
            final ImageObject img = getAssetById(responseAsset.getId(), requestAssets).getImg();
            final Integer type = img != null ? img.getType() : null;
            if (type != null) {
                responseAsset.getImg().setType(type);
            } else {
                final Integer assetId = responseAsset.getId();
                throw new PreBidException(
                        "Response has an Image asset with ID:'%s' present that doesn't exist in the request"
                                .formatted(assetId != null ? assetId : StringUtils.EMPTY));
            }
        }
        if (responseAsset.getData() != null) {
            final DataObject data = getAssetById(responseAsset.getId(), requestAssets).getData();
            final Integer type = data != null ? data.getType() : null;
            if (type != null) {
                responseAsset.getData().setType(type);
            } else {
                throw new PreBidException(
                        "Response has a Data asset with ID:%s present that doesn't exist in the request"
                                .formatted(responseAsset.getId()));
            }
        }
    }

    private static com.iab.openrtb.request.Asset getAssetById(Integer assetId,
                                                              List<com.iab.openrtb.request.Asset> requestAssets) {

        return requestAssets.stream()
                .filter(asset -> Objects.equals(assetId, asset.getId()))
                .findFirst()
                .orElse(com.iab.openrtb.request.Asset.EMPTY);
    }

    private EventsContext createEventsContext(AuctionContext auctionContext) {
        return EventsContext.builder()
                .auctionId(auctionContext.getBidRequest().getId())
                .enabledForAccount(eventsEnabledForAccount(auctionContext))
                .enabledForRequest(eventsEnabledForRequest(auctionContext))
                .auctionTimestamp(auctionTimestamp(auctionContext))
                .integration(integrationFrom(auctionContext))
                .build();
    }

    private static boolean eventsEnabledForAccount(AuctionContext auctionContext) {
        final AccountAuctionConfig accountAuctionConfig = auctionContext.getAccount().getAuction();
        final AccountEventsConfig accountEventsConfig =
                accountAuctionConfig != null ? accountAuctionConfig.getEvents() : null;
        final Boolean accountEventsEnabled = accountEventsConfig != null ? accountEventsConfig.getEnabled() : null;

        return BooleanUtils.isTrue(accountEventsEnabled);
    }

    private static boolean eventsEnabledForRequest(AuctionContext auctionContext) {
        return eventsEnabledForChannel(auctionContext) || eventsAllowedByRequest(auctionContext);
    }

    private static boolean eventsEnabledForChannel(AuctionContext auctionContext) {
        final Map<String, Boolean> channelConfig = Optional.ofNullable(auctionContext.getAccount().getAnalytics())
                .map(AccountAnalyticsConfig::getAuctionEvents)
                .map(AccountAuctionEventConfig::getEvents)
                .orElseGet(AccountAnalyticsConfig::fallbackAuctionEvents);

        final String channelFromRequest = channelFromRequest(auctionContext.getBidRequest());

        return channelConfig.entrySet().stream()
                .filter(entry -> StringUtils.equalsIgnoreCase(channelFromRequest, entry.getKey()))
                .findFirst()
                .map(entry -> BooleanUtils.isTrue(entry.getValue()))
                .orElse(Boolean.FALSE);
    }

    private static String channelFromRequest(BidRequest bidRequest) {
        final ExtRequest ext = bidRequest.getExt();
        final ExtRequestPrebid prebid = ext != null ? ext.getPrebid() : null;
        final ExtRequestPrebidChannel channel = prebid != null ? prebid.getChannel() : null;

        return channel != null ? recogniseChannelName(channel.getName()) : null;
    }

    // TODO: remove alias resolving after transition period
    private static String recogniseChannelName(String channelName) {
        if (StringUtils.equalsIgnoreCase("pbjs", channelName)) {
            return Ortb2ImplicitParametersResolver.WEB_CHANNEL;
        }

        return channelName;
    }

    private static boolean eventsAllowedByRequest(AuctionContext auctionContext) {
        final ExtRequest ext = auctionContext.getBidRequest().getExt();
        final ExtRequestPrebid prebid = ext != null ? ext.getPrebid() : null;

        return prebid != null && prebid.getEvents() != null;
    }

    private long auctionTimestamp(AuctionContext auctionContext) {
        final ExtRequest ext = auctionContext.getBidRequest().getExt();
        final ExtRequestPrebid prebid = ext != null ? ext.getPrebid() : null;
        final Long auctionTimestamp = prebid != null ? prebid.getAuctiontimestamp() : null;

        return auctionTimestamp != null ? auctionTimestamp : clock.millis();
    }

    private static String integrationFrom(AuctionContext auctionContext) {
        final ExtRequest ext = auctionContext.getBidRequest().getExt();
        final ExtRequestPrebid prebid = ext != null ? ext.getPrebid() : null;

        return prebid != null ? prebid.getIntegration() : null;
    }

    private Events createEvents(String bidder,
                                Account account,
                                String bidId,
                                EventsContext eventsContext) {

        return eventsContext.isEnabledForAccount() && eventsContext.isEnabledForRequest()
                ? eventsService.createEvent(
                bidId,
                bidder,
                account.getId(),
                true,
                eventsContext)
                : null;
    }

    private TargetingKeywordsCreator resolveKeywordsCreator(BidType bidType,
                                                            ExtRequestTargeting targeting,
                                                            BidRequest bidRequest,
                                                            Account account,
                                                            Map<String, List<ExtBidderError>> bidWarnings) {

        final Map<BidType, TargetingKeywordsCreator> keywordsCreatorByBidType =
                keywordsCreatorByBidType(targeting, bidRequest, account, bidWarnings);

        return keywordsCreatorByBidType.getOrDefault(
                bidType, keywordsCreator(targeting, bidRequest, account, bidWarnings));
    }

    /**
     * Extracts targeting keywords settings from the bid request and creates {@link TargetingKeywordsCreator}
     * instance if it is present.
     */
    private TargetingKeywordsCreator keywordsCreator(ExtRequestTargeting targeting,
                                                     BidRequest bidRequest,
                                                     Account account,
                                                     Map<String, List<ExtBidderError>> bidWarnings) {

        final JsonNode priceGranularityNode = targeting.getPricegranularity();
        return priceGranularityNode == null || priceGranularityNode.isNull()
                ? null
                : createKeywordsCreator(targeting, priceGranularityNode, bidRequest, account, bidWarnings);
    }

    /**
     * Returns a map of {@link BidType} to correspondent {@link TargetingKeywordsCreator}
     * extracted from {@link ExtRequestTargeting} if it exists.
     */
    private Map<BidType, TargetingKeywordsCreator> keywordsCreatorByBidType(
            ExtRequestTargeting targeting,
            BidRequest bidRequest,
            Account account,
            Map<String, List<ExtBidderError>> bidWarnings) {

        final ExtMediaTypePriceGranularity mediaTypePriceGranularity = targeting.getMediatypepricegranularity();
        if (mediaTypePriceGranularity == null) {
            return Collections.emptyMap();
        }

        final Map<BidType, TargetingKeywordsCreator> result = new EnumMap<>(BidType.class);

        final ObjectNode banner = mediaTypePriceGranularity.getBanner();
        final boolean isBannerNull = banner == null || banner.isNull();
        if (!isBannerNull) {
            result.put(
                    BidType.banner, createKeywordsCreator(targeting, banner, bidRequest, account, bidWarnings));
        }

        final ObjectNode video = mediaTypePriceGranularity.getVideo();
        final boolean isVideoNull = video == null || video.isNull();
        if (!isVideoNull) {
            result.put(
                    BidType.video, createKeywordsCreator(targeting, video, bidRequest, account, bidWarnings));
        }

        final ObjectNode xNative = mediaTypePriceGranularity.getXNative();
        final boolean isNativeNull = xNative == null || xNative.isNull();
        if (!isNativeNull) {
            result.put(
                    BidType.xNative, createKeywordsCreator(targeting, xNative, bidRequest, account, bidWarnings)
            );
        }

        return result;
    }

    private TargetingKeywordsCreator createKeywordsCreator(ExtRequestTargeting targeting,
                                                           JsonNode priceGranularity,
                                                           BidRequest bidRequest,
                                                           Account account,
                                                           Map<String, List<ExtBidderError>> bidWarnings) {
        final int resolvedTruncateAttrChars = resolveTruncateAttrChars(targeting, account);
        final String resolveKeyPrefix = resolveAndValidateKeyPrefix(
                bidRequest, account, resolvedTruncateAttrChars, bidWarnings);

        final String env = Optional.ofNullable(bidRequest.getExt())
                .map(ExtRequest::getPrebid)
                .map(ExtRequestPrebid::getAmp)
                .map(ignored -> TARGETING_ENV_AMP_VALUE)
                .orElse(bidRequest.getApp() == null ? null : TARGETING_ENV_APP_VALUE);

        return TargetingKeywordsCreator.create(
                parsePriceGranularity(priceGranularity),
                BooleanUtils.toBoolean(targeting.getIncludewinners()),
                BooleanUtils.toBoolean(targeting.getIncludebidderkeys()),
                BooleanUtils.toBoolean(targeting.getAlwaysincludedeals()),
                BooleanUtils.isTrue(targeting.getIncludeformat()),
                env,
                resolvedTruncateAttrChars,
                cacheHost,
                cachePath,
                TargetingKeywordsResolver.create(bidRequest, mapper),
                resolveKeyPrefix);
    }

    private int resolveTruncateAttrChars(ExtRequestTargeting targeting, Account account) {
        final AccountAuctionConfig accountAuctionConfig = account.getAuction();
        final Integer accountTruncateTargetAttr =
                accountAuctionConfig != null ? accountAuctionConfig.getTruncateTargetAttr() : null;

        return ObjectUtils.firstNonNull(
                truncateAttrCharsOrNull(targeting.getTruncateattrchars()),
                truncateAttrCharsOrNull(accountTruncateTargetAttr),
                truncateAttrChars);
    }

    /**
     * Returns targeting key prefix.
     * Default prefix for targeting keys used in cases,
     * when correspond value is missing in account auction configuration or bid request ext,
     * or may compose keys longer than 'settings.targeting.truncate-attr-chars' value.
     */
    private static String resolveAndValidateKeyPrefix(BidRequest bidRequest,
                                                      Account account,
                                                      int truncateAttrChars,
                                                      Map<String, List<ExtBidderError>> bidWarnings) {
        final String prefix = Optional.of(bidRequest)
                .map(BidRequest::getExt)
                .map(ExtRequest::getPrebid)
                .map(ExtRequestPrebid::getTargeting)
                .map(ExtRequestTargeting::getPrefix)
                .orElse(Optional.ofNullable(account)
                        .map(Account::getAuction)
                        .map(AccountAuctionConfig::getTargeting)
                        .map(AccountTargetingConfig::getPrefix)
                        .orElse(null));

        final boolean customPrefixIsNotSuitable =
                StringUtils.isNotEmpty(prefix) && prefix.length() + MAX_TARGETING_KEY_LENGTH > truncateAttrChars;
        if (customPrefixIsNotSuitable) {
            final String errorMessage = "Key prefix value is dropped to default. "
                    + "Decrease custom prefix length or increase truncateattrchars by "
                    + (prefix.length() + MAX_TARGETING_KEY_LENGTH - truncateAttrChars);
            bidWarnings.computeIfAbsent("targeting", ignored -> new ArrayList<>())
                    .add(ExtBidderError.of(BidderError.Type.bad_input.getCode(), errorMessage));
        }
        return StringUtils.isEmpty(prefix) || customPrefixIsNotSuitable
                ? DEFAULT_TARGETING_KEY_PREFIX
                : prefix;
    }

    private static Integer truncateAttrCharsOrNull(Integer value) {
        return value != null && value >= 0 && value <= 255 ? value : null;
    }

    private static boolean isCachedDebugEnabled(CachedDebugLog cachedDebugLog) {
        return cachedDebugLog != null && cachedDebugLog.isEnabled();
    }

    private ExtPriceGranularity parsePriceGranularity(JsonNode priceGranularity) {
        try {
            return mapper.mapper().treeToValue(priceGranularity, ExtPriceGranularity.class);
        } catch (JsonProcessingException e) {
            throw new PreBidException(
                    "Error decoding bidRequest.prebid.targeting.pricegranularity: " + e.getMessage(), e);
        }
    }

    private static BidResponse populateSeatNonBid(AuctionContext auctionContext, BidResponse bidResponse) {
        if (!auctionContext.getDebugContext().isShouldReturnAllBidStatuses()) {
            return bidResponse;
        }

        final List<SeatNonBid> seatNonBids = auctionContext.getBidRejectionTrackers().values().stream()
                .flatMap(bidRejectionTracker -> bidRejectionTracker.getRejectedImps().entrySet().stream())
                .collect(Collectors.groupingBy(
                        entry -> entry.getValue().getLeft(),
                        Collectors.mapping(
                                entry -> NonBid.of(entry.getKey(), entry.getValue().getRight()),
                                Collectors.toList())))
                .entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> SeatNonBid.of(entry.getKey(), entry.getValue()))
                .toList();

        final ExtBidResponse updatedExtBidResponse = Optional.ofNullable(bidResponse.getExt())
                .map(ExtBidResponse::toBuilder)
                .orElseGet(ExtBidResponse::builder)
                .seatnonbid(seatNonBids)
                .build();

        return bidResponse.toBuilder().ext(updatedExtBidResponse).build();
    }

    private CacheAsset toCacheAsset(String cacheId) {
        return CacheAsset.of(cacheAssetUrlTemplate.concat(cacheId), cacheId);
    }

    private static <T> Set<T> nullIfEmpty(Set<T> set) {
        if (set.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableSet(set);
    }

    private Optional<ExtBidPrebidVideo> getExtBidPrebidVideo(ObjectNode bidExt) {
        return getExtPrebid(bidExt, ExtBidPrebid.class)
                .map(ExtBidPrebid::getVideo);
    }

    private <T> Optional<T> getExtPrebid(ObjectNode extNode, Class<T> extClass) {
        return Optional.ofNullable(extNode)
                .filter(ext -> ext.hasNonNull(PREBID_EXT))
                .map(ext -> convertValue(extNode, PREBID_EXT, extClass));
    }

    private <T> T convertValue(JsonNode jsonNode, String key, Class<T> typeClass) {
        try {
            return mapper.mapper().convertValue(jsonNode.get(key), typeClass);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private record PaaResult(List<ExtIgi> igis, ExtBidResponseFledge fledge) {
    }
}
