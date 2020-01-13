package hbase.entity;

import java.io.Serializable;

public class TerminalLogHbaseBean implements Serializable {
    /**
     * 默认序列化Id
     */
    private static final long serialVersionUID = 1L;

    private String plateNo;

    private String simNo;

    private long time;

    private int type;

    private String msg;

    public String getPlateNo() {
        return plateNo;
    }

    public void setPlateNo(String plateNo) {
        this.plateNo = plateNo;
    }

    public String getSimNo() {
        return simNo;
    }

    public void setSimNo(String simNo) {
        this.simNo = simNo;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "TerminalLogHbaseBean{" +
                "plateNo='" + plateNo + '\'' +
                ", simNo='" + simNo + '\'' +
                ", time=" + time +
                ", type=" + type +
                ", msg='" + msg + '\'' +
                '}';
    }
}
