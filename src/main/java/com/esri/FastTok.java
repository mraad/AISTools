package com.esri;

/**
 */
public final class FastTok
{
    public String[] tokens = new String[256];

    public int tokenize(
            final String text,
            final char delimiter)
    {
        int count = 0;

        final int newLen = text.length() / 2 + 2;
        if (tokens.length < newLen)
        {
            tokens = new String[newLen];
        }
        int i = 0;
        int j = text.indexOf(delimiter);
        while (j >= 0)
        {
            tokens[count++] = text.substring(i, j);
            i = j + 1;
            j = text.indexOf(delimiter, i);
        }
        tokens[count++] = text.substring(i);

        return count;
    }
}