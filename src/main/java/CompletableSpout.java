public interface CompletableSpout {
    /**
     * @return true if all the tuples have been completed else false.
     */
    public boolean isExhausted();

    /**
     * Cleanup any global state kept
     */
    default public void clean() {
    }

    /**
     * Prepare the spout (globally) before starting the topology
     */
    default public void startup() {
    }
}