package com.github.ddth.cql.qnd;

import java.util.Arrays;

/**
 * AnyThing[] should be instanceof Object[].
 * 
 * @author btnguyen
 */
public class QndArray {

    public static void main(String[] args) {
        String[] tokens = { "1", "2", "3" };
        System.out.println(tokens instanceof Object[]);
        System.out.println(Arrays.asList(tokens));
    }

}
