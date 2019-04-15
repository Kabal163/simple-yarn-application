package com.epam.yarn.container;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Is not used in the current implementation
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class BookingModel implements Serializable {
    private String date;
    private int siteName;
    private int posaContinent;
    private int userLocationCountry;
    private int userLocationCity;
    private double origDestinationDistance;
    private long userId;
    private boolean isMobile;
    private boolean isPackage;
    private int channel;
    private String srch_ci;
    private String srch_co;
    private int srchAdultsCnt;
    private int srchChildrenCnt;
    private int srchRmCnt;
    private long srchDestinationId;
    private long srchDestinationTypeId;
    private boolean isBooking;
    private int cnt;
    private int hotelContinent;
    private int hotelCountry;
    private int hotelMarket;
    private int hotelCluster;
}
