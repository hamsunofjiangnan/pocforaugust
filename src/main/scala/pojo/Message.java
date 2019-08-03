package pojo;

import lombok.Data;

import java.util.Date;

/**
 * @Author: hamsun
 * @Description: 模拟消息
 * @Date: 2019/7/30 23:53
 */
@Data
public class Message {
    private Long id;

    private String msg;

    private int noip;

    private Date sendTime;

}
