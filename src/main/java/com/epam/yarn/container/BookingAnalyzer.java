package com.epam.yarn.container;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BookingAnalyzer {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path src = fs.makeQualified(new Path("/user/files/train.csv"));
        Map<Triple<Integer, Integer, Integer>, Long> hotelId2count = new HashMap<>();
        int badRecordsNumber = 0;

        try (FSDataInputStream in = fs.open(src)) {
            CSVParser parser = CSVParser.parse(in, Charsets.UTF_8, CSVFormat.RFC4180.withHeader());
            for (CSVRecord record : parser) {
                int hotelContinent = -1;
                int hotelCountry = -1;
                int hotelMarket = -1;
                int srchAdutlsCnt = -1;
                boolean isBooking = false;

                for (Map.Entry<String, String> entry : record.toMap().entrySet()) {

                    try {
                        switch (entry.getKey()) {
                            case "hotel_continent":
                                if (StringUtils.isNotEmpty(entry.getValue()))
                                    hotelContinent = Integer.parseInt(entry.getValue());
                                break;
                            case "hotel_country":
                                if (StringUtils.isNotEmpty(entry.getValue()))
                                    hotelCountry = Integer.parseInt(entry.getValue());
                                break;
                            case "hotel_market":
                                if (StringUtils.isNotEmpty(entry.getValue()))
                                    hotelMarket = Integer.parseInt(entry.getValue());
                                break;
                            case "srch_adults_cnt":
                                if (StringUtils.isNotEmpty(entry.getValue()))
                                    srchAdutlsCnt = Integer.parseInt(entry.getValue());
                                break;
                            case "is_booking":
                                if (StringUtils.isNotEmpty(entry.getValue()))
                                    isBooking = Integer.parseInt(entry.getValue()) == 1;
                                break;
                        }
                    } catch (NumberFormatException e) {
                        badRecordsNumber++;
                        break;
                    }
                }

                // srchAdutlsCnt must equal 2 because we are searching for booking records of couples
                if (hotelContinent == -1 || hotelCountry == -1 || hotelMarket == -1 || srchAdutlsCnt != 2 || !isBooking)
                    continue;

                Triple<Integer, Integer, Integer> hotelId = Triple.of(hotelContinent, hotelCountry, hotelMarket);
                Long count = hotelId2count.getOrDefault(hotelId, 0L);
                hotelId2count.put(hotelId, ++count);
            }

            hotelId2count.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                    .limit(3)
                    .forEach(System.out::println);
            System.out.println("Amount of bad records: " + badRecordsNumber);
        }
    }
}
