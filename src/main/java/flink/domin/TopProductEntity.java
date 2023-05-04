package flink.domin;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;

@Log
@Getter
@Setter
public class TopProductEntity {

    private String productId;
    private int actionTimes;
    private long windowEnd;
    private long windowStart;
    private String rankName;

    public static TopProductEntity of(String itemId,long start,long end, Long count) {
        TopProductEntity res = new TopProductEntity();
        res.setActionTimes(count.intValue());
        res.setProductId(itemId);
        res.setWindowStart(start);
        res.setWindowEnd(end);
        res.setRankName(String.valueOf(end));
        return res;
    }

    @Override
    public String toString() {
        return "TopProductEntity{" +
                "productId=" + productId +
                ", actionTimes=" + actionTimes +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
