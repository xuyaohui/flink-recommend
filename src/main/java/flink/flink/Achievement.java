package flink.flink;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class Achievement  implements Serializable {

    private static final long serialVersionUID = -1L;

    private String name;

    private Integer mathVal;

    private Integer physicsVal;
}
