package mongoDB.utils;

import com.mongodb.DBObject;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class MongoBeanUtils {

    public static <T> T dbObjectToBean(DBObject dbObject, T bean) throws InvocationTargetException, IllegalAccessException {
        if (dbObject == null) {
            System.out.println("[MongoBeanUtils]DBObject is null!");
            return null;
        } else {
            Field[] fields = bean.getClass().getDeclaredFields();
            for (Field field : fields) {
                String varName = field.getName();
                Object object = dbObject.get(varName);
                if (object == null) {
//                    System.out.println("[MongoBeanUtils]Object" + varName + " is null!");
                    continue;
                }
                BeanUtils.setProperty(bean, varName, object);
            }
        }
        return bean;
    }
}
