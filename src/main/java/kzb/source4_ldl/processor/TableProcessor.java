package kzb.source4_ldl.processor;

public interface TableProcessor {
    void process();
    String getProcessorName();
    String getDescription();
}