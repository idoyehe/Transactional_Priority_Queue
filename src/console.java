import java.util.ArrayList;

public class console {
    public static void main(String[] args) {
        int x = 46546;
        System.out.println(Integer.toBinaryString(x));
        ArrayList<Integer> arr = new ArrayList<>();
        for (int i = 1; i < Integer.toBinaryString(x).length(); i++) {
            arr.add(Character.getNumericValue(Integer.toBinaryString(x).charAt(i)));
        }
        System.out.println(arr.toString());

    }
}
