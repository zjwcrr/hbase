package mongoDB.enums;

import java.util.HashMap;
import java.util.Map;

public enum TerminalLogEnum {
    REGISTERED("终端注册", 1),
    ONELINE("终端上线", 2),
    OFFLINE("终端下线", 3);

    private static Map<String, TerminalLogEnum> terminalLogNameMap = new HashMap<String, TerminalLogEnum>();
    private static Map<Integer, TerminalLogEnum> terminalLogValueMap = new HashMap<Integer, TerminalLogEnum>();

    static {
        for (TerminalLogEnum eum : TerminalLogEnum.values()) {
            terminalLogNameMap.put(eum.getTypeName(), eum);
            terminalLogValueMap.put(eum.getTypeValue(), eum);
        }
    }

    /**
     * 类型名称
     */
    private String typeName;

    /**
     * 类型值
     */
    private int typeValue;

    /**
     * 构造函数
     *
     * @param typeName  类型名称
     * @param typeValue 类型值
     */
    private TerminalLogEnum(String typeName, int typeValue) {
        this.typeName = typeName;
        this.typeValue = typeValue;
    }

    /**
     * 根据类型名称和获取类型值
     *
     * @param typeName 类型名称
     * @return 类型值
     */
    public static int getTypeValueByName(String typeName) {
        return terminalLogNameMap.get(typeName).typeValue;
    }

    /**
     * 根据类型值获取类型名称
     *
     * @param typeValue 类型值
     * @return 类型名称
     */
    public static String getTypeNameByValue(int typeValue) {
        return terminalLogValueMap.get(typeValue).typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getTypeValue() {
        return typeValue;
    }

    public void setTypeValue(int typeValue) {
        this.typeValue = typeValue;
    }
}
