package org.apache.drill.exec.store.druid.druid;

public class PagingIdentifier {

    private String _segmentName;
    private int _segmentOffset;

    public PagingIdentifier(String segmentName, int segmentOffset)
    {
        _segmentName = segmentName;
        _segmentOffset = segmentOffset;
    }

    public String getSegmentName()
    {
        return _segmentName;
    }

    public int getSegmentOffset()
    {
        return _segmentOffset;
    }
}
