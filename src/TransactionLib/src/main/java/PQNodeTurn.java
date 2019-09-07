package TransactionLib.src.main.java;

public enum PQNodeTurn {
    LEFT(0), RIGHT(1);

    private final Integer value;

    private PQNodeTurn(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return this.value;
    }
}
