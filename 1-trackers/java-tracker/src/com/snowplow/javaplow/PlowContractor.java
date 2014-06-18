/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplow.javaplow;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * Contractor Class
 * The contractor class is used to create a standard list of assertions that abide by the
 *  snowplow conventions. This ensures that in the following steps of snowplow data processing
 *  all information needed will be present.
 *
 * Contractor Class, mostly used to assert preconditions
 *  Contractor to verify preconditions, post conditions and invariants.
 *  Uses data structure to hold custom ID contracts for specific jobs
 *
 * @version 0.1.0
 * @author Kevin Gleason
 */

public class PlowContractor<T> {
    //Class variables
    private static final String[] SUPPORTED_PLATFORMS = {"pc", "tv", "mob", "cnsl", "iot"};

    //Instance Variables
    private Map<String, Function<T>> customContracts = new HashMap<String, Function<T>>();

    /**
     * Contract Checker
     *   Checks the preconditions and post conditions depending on how it is configured
     *   Can be enabled or disabled as needed with first argument which will be passed by
     *   the Tracker instance.
     * @param checkContracts boolean function passed from Tracker instance, if false contracts
     *                       are turned off and all checks return true.
     * @param function The name of the function to check. Currently three are implemented
     *                 and custom functions are allowed.
     * @param input The input for the function being called and checked if it abides by the
     *              contract.
     * @return True or False depending on whether or not the input abides by the rules of the contract.
     */
    public boolean checkContract(boolean checkContracts, Function<T> function, T input){
        boolean goodContract = checkContracts && function.functionCheck(input) || !checkContracts;
        if (!goodContract)
            throw new Error(function.getErrorMsg());
        return true;
    }

    /**
     * Make a custom contract --Future
     * Make an implementation of the Function interface and pass it in to the custom contract input.
     * This will put it in a Map which can be looked up with checkCustomContract
     * @param name Custom name ID of the contract.
     * @param function The implementation of the Function interface.
     */
    public void customContract(String name, Function<T> function){
        this.customContracts.put(name, function);
    }

    /**
     * Used to check a custom contract made using the customContract function.
     * @param checkContracts Passed in value from TrackerC, if false then checkCustom will always return true
     * @param name The custom ID of your contract.
     * @param input The input to be checked using the contract.
     * @return
     */
    public boolean checkCustomContract(boolean checkContracts, String name, T input){
        Function<T> function = this.customContracts.get(name);
        boolean goodContract = checkContracts && function.functionCheck(input) || !checkContracts;
        if (!goodContract)
            throw new Error(function.getErrorMsg());
        return true;
    }


    /**
     * Slight work-around due to lack of lambda functions. Programmed in Java7 for sake of compatibility.
     * @param <T> Type of function depends on the type of the Contractor. Each contractor can only check
     *           its respective type.
     */
    public interface Function<T> {
        /**
         * The checker function. The implementation of the contract.
         * @param input The info to be checked against the contract.
         * @return True or false depending on whether the input abides by the contract.
         */
        public boolean functionCheck(T input);

        /**
         * The error message returned when a contract is broken.
         * @return String error message thrown.
         */
        public String getErrorMsg();
    }


    /**
     * Non Empty String
     * Some pieces of the Snowplow enrichment process would be thrown off if certain fields were empty.
     * This function ensured that the string input fits the snowplow standards where required.
     */
    public static final Function<String> non_empty_string = new Function<String>() {
        public boolean functionCheck(String input){
            return !input.isEmpty() && input.length() > 0;
        }
        public String getErrorMsg(){
            return "String Error - Field cannot be empty string.";
        }
    };

    /**
     * Is Supported Platform
     * Currently snowplow only supports a few platforms.
     * @see com.snowplow.javaplow.TrackerC
     */
    public static final Function<String> is_supported_platform = new Function<String>(){
        public boolean functionCheck(String input){
            for (String i : SUPPORTED_PLATFORMS)
                if (input.equals(i))
                    return true;
            return false;
        }
        public String getErrorMsg(){
            return "Platform Error - Platform is not supported.";
        }
    };

    /**
     * Non Empty Dictionary
     * Used to assert that item dictionaries fit the required snowplow standards --Future
     */
    public static final Function<String> non_empty_dict = new Function<String>() {
        public boolean functionCheck(String input) {
            return !input.equals("{}") && input.length() > 2;
        }
        public String getErrorMsg() {
            return "Dictionary Error - Cannot use empty dictionary as input.";
        }
    };

    /**
     * Positive Number
     * Used to assert that a number field that is required to be positive, is indeed positive.
     */
    public static final Function<Integer> positive_number = new Function<Integer>() {
        public boolean functionCheck(Integer input){
            return input > 0;
        }
        public String getErrorMsg(){
            return "Integer Error - Field cannot be negative number.";
        }
    };

    public static void main(String[] args){
        //Test cases - contracts enables or disables all contracts
        boolean contracts = false;
        PlowContractor<String> stringContractor = new PlowContractor<String>();
        PlowContractor<Integer> integerContractor = new PlowContractor<Integer>();

        //Able to make custom contracts like so
        PlowContractor.Function<String> string_is_long = new PlowContractor.Function<String>() {
            public boolean functionCheck(String input){
                return !input.isEmpty() && input.length() > 10;
            }
            public String getErrorMsg(){
                return "String Error - Input cannot be a string under length 10.";
            }
        };

        //Check with checkCustomContract
        stringContractor.customContract("dict", string_is_long);
        assert stringContractor.checkCustomContract(contracts,"dict","Hello I Am Kevin");
        assert integerContractor.checkContract(contracts, PlowContractor.positive_number, -10);
        System.out.println((System.currentTimeMillis()/10));
        Date date = new Date(System.currentTimeMillis());
        System.out.println(date.toString());
    }
}
