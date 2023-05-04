package flink.domin;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogEntity {

    private String userId;
    private String productId;
    private Long time;
    private String action;
}
