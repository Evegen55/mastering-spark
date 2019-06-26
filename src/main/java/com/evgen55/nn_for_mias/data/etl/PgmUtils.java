package com.evgen55.nn_for_mias.data.etl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class PgmUtils {

    public static String PGM_SUFFIX = ".pgm";

    /**
     * Magic number representing the binary PGM file type.
     */
    public static final String MAGIC = "P5";
    /**
     * Character indicating a comment.
     */
    public static final char COMMENT = '#';

    public static final int MAX_GREY_VAL = 255;

    public static final int IMAGE_WIDTH = 1024;

    public static final int IMAGE_HIGH = 1024;

    public static String nextString(final InputStream stream) throws IOException {
        final List<Byte> bytes = new ArrayList<>();
        while (true) {
            final int b = stream.read();
            if (b != -1) {
                final char c = (char) b;
                if (c == COMMENT) {
                    skipComment(stream);
                } else if (!Character.isWhitespace(c)) {
                    bytes.add((byte) b);
                } else if (bytes.size() > 0) {
                    break;
                }
            } else {
                break;
            }

        }
        final byte[] bytesArray = new byte[bytes.size()];
        for (int i = 0; i < bytesArray.length; ++i) {
            bytesArray[i] = bytes.get(i);
        }

        return new String(bytesArray);
    }

    public static int skipComment(final InputStream stream) throws IOException {
        int skipPos = 0;
        int d;
        do {
            d = stream.read();
            skipPos += d;
        } while (d != -1 && d != '\n' && d != '\r');
        return skipPos;
    }

    public static void writeImageFromArray(final int[][] image, final int maxval, final File file) throws IOException {
        if (maxval > MAX_GREY_VAL)
            throw new IllegalArgumentException("The maximum gray value cannot exceed " + MAX_GREY_VAL + ".");
        try (final BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(file))) {

            final int imageWidth = image[0].length;
            final int imageHigh = image.length;

            stream.write(MAGIC.getBytes());
            stream.write("\n".getBytes());
            stream.write(Integer.toString(imageWidth).getBytes());
            stream.write(" ".getBytes());
            stream.write(Integer.toString(imageHigh).getBytes());
            stream.write("\n".getBytes());
            stream.write(Integer.toString(maxval).getBytes());
            stream.write("\n".getBytes());
            for (int i = 0; i < imageHigh; ++i) {
                for (int j = 0; j < imageWidth; ++j) {
                    final int p = image[i][j];
                    if (p < 0 || p > maxval)
                        throw new IOException("Pixel value " + p + " outside of range [0, " + maxval + "].");
                    stream.write(image[i][j]);
                }
            }
        }
    }

    public static void writeImageFromArray(final double[] image,
                                           final int width, final int height,
                                           final int maxval, final File file) throws IOException {
        if (maxval > MAX_GREY_VAL)
            throw new IllegalArgumentException("The maximum gray value cannot exceed " + MAX_GREY_VAL + ".");
        try (final BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(file))) {
            stream.write(MAGIC.getBytes());
            stream.write("\n".getBytes());
            stream.write(Integer.toString(width).getBytes());
            stream.write(" ".getBytes());
            stream.write(Integer.toString(height).getBytes());
            stream.write("\n".getBytes());
            stream.write(Integer.toString(maxval).getBytes());
            stream.write("\n".getBytes());
            for (int i = 0; i < image.length; ++i) {
                    final int p = (int) image[i];
                    if (p < 0 || p > maxval)
                        throw new IOException("Pixel value " + p + " outside of range [0, " + maxval + "].");
                    stream.write(p);
            }
        }
    }

    public static double[] readAsSingleDimArrayFromFile(final BufferedInputStream inImageStream,
                                                         final int maxGreyValue, final int recurrentImageBufferSize) throws IOException {
        final double[] image = new double[recurrentImageBufferSize];
        for (int i = 0; i < recurrentImageBufferSize; ++i) {
            final int p = inImageStream.read();
            if (p == -1)
                throw new IOException("Reached end-of-file prematurely.");
            else if (p > maxGreyValue)
                throw new IOException("Pixel value " + p + " outside of range [0, " + maxGreyValue + "].");
            image[i] = p; //upscale from 1 byte to 8 - CRAZY JAVA
        }
        return image;
    }

    public static double[] readAsSingleDimArrayMirrorFromFile(final BufferedInputStream inImageStream,
                                                              final int width, final int height,
                                                              final int maxGreyValue, final int recurrentImageBufferSize) throws IOException {
        final double[] image = new double[recurrentImageBufferSize];
        for (int i = 0; i < height; ++i) {
            for (int j = 0; j < width; ++j) {
                final int p = inImageStream.read();
                if (p == -1)
                    throw new IOException("Reached end-of-file prematurely.");
                else if (p > maxGreyValue)
                    throw new IOException("Pixel value " + p + " outside of range [0, " + maxGreyValue + "].");
                final int index = width - 1 - j + width*i;
                image[index] = p;
            }
        }
        return image;
    }

    public static int[][] readAsTwoDimArrayIntsFromFile(final BufferedInputStream inImageStream,
                                                         final int width, final int height,
                                                         final int widthLimiterFromRight, final int maxGreyValue) throws IOException {
        final int widthCorrected = width - widthLimiterFromRight;
        System.out.println("read image with width " + widthCorrected + " x " + height + " high and the maximum gray value " + maxGreyValue + ".");
        final int[][] image = new int[height][widthCorrected];
        for (int i = 0; i < height; ++i) {
            for (int j = 0; j < widthCorrected; ++j) {
                final int p = inImageStream.read();
                if (p == -1)
                    throw new IOException("Reached end-of-file prematurely.");
                else if (p > maxGreyValue)
                    throw new IOException("Pixel value " + p + " outside of range [0, " + maxGreyValue + "].");
                image[i][j] = p;
            }
            inImageStream.skip(widthLimiterFromRight);
        }
        return image;
    }

    public static int[][] readAsTwoDimArrayIntsMirrorFromFile(final BufferedInputStream inImageStream,
                                                               final int width, final int height, final int maxGreyValue) throws IOException {
        System.out.println("read image with width " + width + " x " + height + "high and the maximum gray value " + maxGreyValue + ".");
        final int[][] image = new int[height][width];
        for (int i = 0; i < height; ++i) {
            for (int j = 0; j < width; ++j) {
                final int p = inImageStream.read();
                if (p == -1)
                    throw new IOException("Reached end-of-file prematurely.");
                else if (p > maxGreyValue)
                    throw new IOException("Pixel value " + p + " outside of range [0, " + maxGreyValue + "].");
                image[i][width - 1 - j] = p;
            }
        }
        return image;
    }

}
