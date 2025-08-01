package org.prebid.server.bidder.gumgum;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.bidder.Bidder;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderCall;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtImpPrebid;
import org.prebid.server.proto.openrtb.ext.request.gumgum.ExtImpGumgum;
import org.prebid.server.proto.openrtb.ext.request.gumgum.ExtImpGumgumBanner;
import org.prebid.server.proto.openrtb.ext.request.gumgum.ExtImpGumgumVideo;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.util.BidderUtil;
import org.prebid.server.util.HttpUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GumgumBidder implements Bidder<BidRequest> {

    private static final String REQUEST_EXT_PRODUCT = "product";

    private static final TypeReference<ExtPrebid<ExtImpPrebid, ExtImpGumgum>> GUMGUM_EXT_TYPE_REFERENCE =
            new TypeReference<>() {
            };

    private final String endpointUrl;
    private final JacksonMapper mapper;

    public GumgumBidder(String endpointUrl, JacksonMapper mapper) {
        this.endpointUrl = HttpUtil.validateUrl(Objects.requireNonNull(endpointUrl));
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public Result<List<HttpRequest<BidRequest>>> makeHttpRequests(BidRequest bidRequest) {
        final List<BidderError> errors = new ArrayList<>();

        final BidRequest outgoingRequest;
        try {
            outgoingRequest = createBidRequest(bidRequest, errors);
        } catch (PreBidException e) {
            errors.add(BidderError.badInput(e.getMessage()));
            return Result.withErrors(errors);
        }

        return Result.of(Collections.singletonList(BidderUtil.defaultRequest(outgoingRequest, endpointUrl, mapper)),
                errors);
    }

    private BidRequest createBidRequest(BidRequest bidRequest, List<BidderError> errors) {
        final List<Imp> modifiedImps = new ArrayList<>();
        String zone = null;
        BigInteger pubId = null;

        for (Imp imp : bidRequest.getImp()) {
            try {
                final ExtPrebid<ExtImpPrebid, ExtImpGumgum> extImp = parseImpExt(imp);
                final ExtImpGumgum extImpGumgum = extImp.getBidder();
                final String adUnitCode = Optional.ofNullable(extImp.getPrebid())
                        .map(ExtImpPrebid::getAdUnitCode)
                        .orElse(null);

                modifiedImps.add(modifyImp(imp, extImpGumgum, adUnitCode));

                final String extZone = extImpGumgum.getZone();
                if (StringUtils.isNotEmpty(extZone)) {
                    zone = extZone;
                }

                final BigInteger extPubId = extImpGumgum.getPubId();
                if (extPubId != null && !extPubId.equals(BigInteger.ZERO)) {
                    pubId = extPubId;
                }

            } catch (PreBidException e) {
                errors.add(BidderError.badInput(e.getMessage()));
            }
        }

        if (modifiedImps.isEmpty()) {
            throw new PreBidException("No valid impressions");
        }

        return bidRequest.toBuilder()
                .imp(modifiedImps)
                .site(modifySite(bidRequest.getSite(), zone, pubId))
                .build();
    }

    private ExtPrebid<ExtImpPrebid, ExtImpGumgum> parseImpExt(Imp imp) {
        try {
            return mapper.mapper().convertValue(imp.getExt(), GUMGUM_EXT_TYPE_REFERENCE);
        } catch (IllegalArgumentException e) {
            throw new PreBidException(e.getMessage());
        }
    }

    private Imp modifyImp(Imp imp, ExtImpGumgum extImp, String adUnitCode) {
        final Imp.ImpBuilder impBuilder = imp.toBuilder();

        final String product = extImp.getProduct();
        if (StringUtils.isNotEmpty(product)) {
            final ObjectNode productExt = mapper.mapper().createObjectNode().put(REQUEST_EXT_PRODUCT, product);
            impBuilder.ext(productExt);
        }

        if (StringUtils.isNotEmpty(adUnitCode)) {
            impBuilder.tagid(adUnitCode);
        }

        final Banner banner = imp.getBanner();
        if (banner != null) {
            final Banner resolvedBanner = resolveBanner(banner, extImp);
            if (resolvedBanner != null) {
                impBuilder.banner(resolvedBanner);
            }
        }

        final Video video = imp.getVideo();
        if (video != null) {
            final String irisId = extImp.getIrisId();
            if (StringUtils.isNotEmpty(irisId)) {
                final Video resolvedVideo = resolveVideo(video, irisId);
                impBuilder.video(resolvedVideo);
            }
        }

        return impBuilder.build();
    }

    private Banner resolveBanner(Banner banner, ExtImpGumgum extImpGumgum) {
        final List<Format> format = banner.getFormat();
        if (banner.getH() == null && banner.getW() == null && CollectionUtils.isNotEmpty(format)) {
            final Format firstFormat = format.getFirst();

            final Long slot = extImpGumgum.getSlot();
            final ObjectNode bannerExt = slot != null && slot != 0L
                    ? mapper.mapper().valueToTree(resolveBannerExt(format, slot))
                    : banner.getExt();

            return banner.toBuilder()
                    .w(firstFormat.getW())
                    .h(firstFormat.getH())
                    .ext(bannerExt)
                    .build();
        }
        return null;
    }

    private static ExtImpGumgumBanner resolveBannerExt(List<Format> formats, Long slot) {
        return formats.stream()
                .filter(format -> ObjectUtils.allNotNull(format.getW(), format.getH()))
                .max(Comparator.comparing((Format format) -> Math.max(format.getW(), format.getH()))
                        .thenComparing(Format::getW)
                        .thenComparing(Format::getH))
                .map(format -> ExtImpGumgumBanner.of(slot, format.getW(), format.getH()))
                .orElseGet(() -> ExtImpGumgumBanner.of(slot, 0, 0));
    }

    private Video resolveVideo(Video video, String irisId) {
        final ObjectNode videoExt = mapper.mapper().valueToTree(ExtImpGumgumVideo.of(irisId));
        return video.toBuilder().ext(videoExt).build();
    }

    private static Site modifySite(Site requestSite, String zone, BigInteger pubId) {
        if (requestSite == null) {
            return null;
        }

        final Site.SiteBuilder modifiedSite = requestSite.toBuilder();
        if (StringUtils.isNotEmpty(zone)) {
            modifiedSite.id(zone);
        }
        if (pubId != null && !pubId.equals(BigInteger.ZERO)) {
            final Publisher publisher = requestSite.getPublisher();
            final Publisher.PublisherBuilder publisherBuilder = publisher != null
                    ? publisher.toBuilder() : Publisher.builder();
            modifiedSite.publisher(publisherBuilder.id(pubId.toString()).build());
        }
        return modifiedSite.build();
    }

    @Override
    public Result<List<BidderBid>> makeBids(BidderCall<BidRequest> httpCall, BidRequest bidRequest) {
        try {
            final BidResponse bidResponse = mapper.decodeValue(httpCall.getResponse().getBody(), BidResponse.class);
            return Result.of(extractBids(bidResponse, bidRequest), Collections.emptyList());
        } catch (DecodeException | PreBidException e) {
            return Result.withError(BidderError.badServerResponse(e.getMessage()));
        }
    }

    private static List<BidderBid> extractBids(BidResponse bidResponse, BidRequest bidRequest) {
        return bidResponse == null || CollectionUtils.isEmpty(bidResponse.getSeatbid())
                ? Collections.emptyList()
                : bidsFromResponse(bidResponse, bidRequest);
    }

    private static List<BidderBid> bidsFromResponse(BidResponse bidResponse, BidRequest bidRequest) {
        return bidResponse.getSeatbid().stream()
                .filter(Objects::nonNull)
                .map(SeatBid::getBid)
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(bid -> toBidderBid(bid, bidRequest, bidResponse.getCur()))
                .toList();
    }

    private static BidderBid toBidderBid(Bid bid, BidRequest bidRequest, String currency) {
        final BidType bidType = getBidType(bid.getImpid(), bidRequest.getImp());
        final Bid updatedBid = bidType == BidType.video
                ? bid.toBuilder().adm(resolveAdm(bid.getAdm(), bid.getPrice())).build()
                : bid;
        return BidderBid.of(updatedBid, bidType, currency);
    }

    private static BidType getBidType(String impId, List<Imp> imps) {
        for (Imp imp : imps) {
            if (imp.getId().equals(impId)) {
                return imp.getBanner() != null ? BidType.banner : BidType.video;
            }
        }
        return BidType.video;
    }

    private static String resolveAdm(String bidAdm, BigDecimal price) {
        return StringUtils.isNotBlank(bidAdm) ? bidAdm.replace("${AUCTION_PRICE}", String.valueOf(price)) : bidAdm;
    }
}
