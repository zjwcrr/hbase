package mongoDB.entity;

import java.io.Serializable;

public class TerminalLogEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    private long time;

    private long endTime;

    private String type;

    private String typeName;

    private String ip;

    private Object params;

    private Object result;

    private String userCode;

    private String userName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Object getParams() {
        return params;
    }

    public void setParams(Object params) {
        this.params = params;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return "TerminalLogEntity{" +
                "id='" + id + '\'' +
                ", time=" + time +
                ", endTime=" + endTime +
                ", type='" + type + '\'' +
                ", typeName='" + typeName + '\'' +
                ", ip='" + ip + '\'' +
                ", params=" + params +
                ", result=" + result +
                ", userCode='" + userCode + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}
