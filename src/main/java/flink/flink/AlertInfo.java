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
public class AlertInfo implements Serializable {

    private static final long serialVersionUID = -1L;

    private String name;

    private String descInfo;

}
