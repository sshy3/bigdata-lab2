package com.example.sample;


import android.bluetooth.BluetoothGattCharacteristic;

/**
 * Created by Prakash Reddy Vaka
 */
public class SensorTagData {

    public static double extractHumAmbientTemperature(BluetoothGattCharacteristic c) {
        int rawT = shortSignedAtOffset(c, 0);

        return -46.85 + 175.72/65536 *(double)rawT;
    }

    public static double extractHumidity(BluetoothGattCharacteristic c) {
        int a = shortUnsignedAtOffset(c, 2);
        // bits [1..0] are status bits and need to be cleared
        a = a - (a % 4);

        return ((-6f) + 125f * (a / 65535f));
    }

    public static int[] extractCalibrationCoefficients(BluetoothGattCharacteristic c) {
        int[] coefficients = new int[8];

        coefficients[0] = shortUnsignedAtOffset(c, 0);
        coefficients[1] = shortUnsignedAtOffset(c, 2);
        coefficients[2] = shortUnsignedAtOffset(c, 4);
        coefficients[3] = shortUnsignedAtOffset(c, 6);
        coefficients[4] = shortSignedAtOffset(c, 8);
        coefficients[5] = shortSignedAtOffset(c, 10);
        coefficients[6] = shortSignedAtOffset(c, 12);
        coefficients[7] = shortSignedAtOffset(c, 14);

        return coefficients;
    }

    public static double extractBarTemperature(BluetoothGattCharacteristic characteristic, final int[] c) {
        // c holds the calibration coefficients

        int t_r;	// Temperature raw value from sensor
        double t_a; // Temperature actual value in unit centi degrees celsius

        t_r = shortSignedAtOffset(characteristic, 0);

        t_a = (100 * (c[0] * t_r / Math.pow(2,8) + c[1] * Math.pow(2,6))) / Math.pow(2,16);

        return t_a / 100;
    }

    public static double extractBarometer(BluetoothGattCharacteristic characteristic, final int[] c) {
        // c holds the calibration coefficients

        int t_r;	// Temperature raw value from sensor
        int p_r;	// Pressure raw value from sensor
        double S;	// Interim value in calculation
        double O;	// Interim value in calculation
        double p_a; // Pressure actual value in unit Pascal.

        t_r = shortSignedAtOffset(characteristic, 0);
        p_r = shortUnsignedAtOffset(characteristic, 2);


        S = c[2] + c[3] * t_r / Math.pow(2,17) + ((c[4] * t_r / Math.pow(2,15)) * t_r) / Math.pow(2,19);
        O = c[5] * Math.pow(2,14) + c[6] * t_r / Math.pow(2,3) + ((c[7] * t_r / Math.pow(2,15)) * t_r) / Math.pow(2,4);
        p_a = (S * p_r + O) / Math.pow(2,14);

        //Convert pascal to in. Hg
        double p_hg = p_a * 0.000296;

        return p_hg;
    }
    
    /**
     * Gyroscope, Magnetometer, Barometer, IR temperature
     * all store 16 bit two's complement values in the awkward format
     * LSB MSB, which cannot be directly parsed as getIntValue(FORMAT_SINT16, offset)
     * because the bytes are stored in the "wrong" direction.
     *
     * This function extracts these 16 bit two's complement values.
     * */
    private static Integer shortSignedAtOffset(BluetoothGattCharacteristic c, int offset) {
        Integer lowerByte = c.getIntValue(BluetoothGattCharacteristic.FORMAT_UINT8, offset);
        Integer upperByte = c.getIntValue(BluetoothGattCharacteristic.FORMAT_SINT8, offset + 1); // Note: interpret MSB as signed.

        return (upperByte << 8) + lowerByte;
    }

    private static Integer shortUnsignedAtOffset(BluetoothGattCharacteristic c, int offset) {
        Integer lowerByte = c.getIntValue(BluetoothGattCharacteristic.FORMAT_UINT8, offset);
        Integer upperByte = c.getIntValue(BluetoothGattCharacteristic.FORMAT_UINT8, offset + 1); // Note: interpret MSB as unsigned.

        return (upperByte << 8) + lowerByte;
    }
    public static Float[] extractAccelerometerReading(final BluetoothGattCharacteristic c, int offset){
    /*
     * The accelerometer has the range [-2g, 2g] with unit (1/64)g.
     *
     * To convert from unit (1/64)g to unit g we divide by 64.
     *
     * (g = 9.81 m/s^2)
     *
     * The z value is multiplied with -1 to coincide
     * with how we have arbitrarily defined the positive y direction.
     * (illustrated by the apps accelerometer image)
     * */
        Integer x = c.getIntValue(BluetoothGattCharacteristic.FORMAT_SINT8, offset);
        Integer y = c.getIntValue(BluetoothGattCharacteristic.FORMAT_SINT8, offset+1);
        Integer z = c.getIntValue(BluetoothGattCharacteristic.FORMAT_SINT8, offset+2) * -1;
        
        float scaledX = x / 64.0f;
        float scaledY = y / 64.0f;
        float scaledZ = z / 64.0f;
        Float[] data = {scaledX,scaledY,scaledZ} ;
        
        return data;
    }

    public static Float[] extractGyroscopeReading(final BluetoothGattCharacteristic c, int offset){

        float y = shortSignedAtOffset(c, offset) * (500f / 65536f) * -1;
        float x = shortSignedAtOffset(c, offset+2) * (500f / 65536f);
        float z = shortSignedAtOffset(c, offset+4) * (500f / 65536f);
        Float[] data ={x,y,z};
        return data;
    }

}