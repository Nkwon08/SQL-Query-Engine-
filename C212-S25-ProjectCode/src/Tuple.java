import java.util.HashMap;
import java.util.Map;

/**
 * A tuple is an ordered collection of Objects and their associated types (Integer, Double or String)
 * Objects are stored in an array while types are stored in a map of (index, type)
 *
 */
public class Tuple implements ITuple {
    private Object[] values;
    private Map<Integer, Class<?>> typeMap;

    /**
     * The constructor receives a schema and creates the object array and typemap (representing the tuple)
     * The schema has the types of attributes stored as strings ("Integer", "Double", "String")
     * Based upon these types the constructor stores the actual class (Integer.class, Double.class, String.class) to the typemap
     * @param schema
     */
    public Tuple(ISchema schema) {
        Map<Integer, String> attributes = schema.getAttributes();
        values = new Object[attributes.size()];
        typeMap = new HashMap<>();

        for (Map.Entry<Integer, String> entry : attributes.entrySet()) {
            int index = entry.getKey();
            String value = entry.getValue();

            String[] parts = value.split(":");
            if (parts.length != 2) {
                System.err.println("Invalid schema entry: " + value);
                continue;
            }

            String typeString = parts[1].trim();

            if (typeString.equals("Integer")) {
                typeMap.put(index, Integer.class);
            } else if (typeString.equals("Double")) {
                typeMap.put(index, Double.class);
            } else if (typeString.equals("String")) {
                typeMap.put(index, String.class);
            }
        }
}
    /**
     * Stores the value at the given index in the (tuple) object
     * The value is converted from the object to to its actual class from the typemap
     * @param index
     * @param value
     */
    @Override

    public void setValue(int index, Object value) {
        Class<?> expectedType = typeMap.get(index);
        Object convert = null;

        if (expectedType.equals(Integer.class)){
            convert = Integer.valueOf(value.toString());
        }
        if (expectedType.equals(Double.class)){
            convert = Double.valueOf(value.toString());
        }
        if (expectedType.equals(String.class)){
            convert = value.toString();
        }
        values[index] = convert;
    }

    /**
     * Returns the value at a given index from the tuple object
     * @param index
     * @return
     * @param <T>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(int index) {
        Object value = values[index];
        return (T) value;

    }

    /**
     * Returns the tuple as an array of Objects
     * @return
     */
    @Override
    public Object[] getValues() {
        return values;

    }

    /**
     * Sets the tuple values to the provided ones
     * The values are converted from objects to their actual classes from the typemap
     * @param values
     */
    @Override
    public void setValues(Object[] values) {
        for (int i = 0; i < values.length; i++){
            setValue(i, values[i]);
        }

    }
}