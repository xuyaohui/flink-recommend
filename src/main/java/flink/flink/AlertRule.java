package flink.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class AlertRule implements Serializable {

    private static final long serialVersionUID = -1L;

    private String target;

    //0小于 1等于 2大于
    private Integer type;

    private Integer criticalVal;

    private String descInfo;
}
