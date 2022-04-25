package com.lcl.scs.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.lcl.scs.util.logging.LoggingUtilities;

public class DateFormater {
	private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static final String DATE_FORMAT_YYYYMMDDHHmmSS = "yyyyMMddHHmmss";
	

	public static ZonedDateTime timeZoneConvertion(Date inputDate, String fromTimeZoneId, String toTimeZoneId)
			throws Exception {

		try {
			ZoneId fromZone = ZoneId.of(fromTimeZoneId);
			LocalDateTime input_local_time = LocalDateTime.ofInstant(inputDate.toInstant(), fromZone);
			ZonedDateTime inputZonedDateTime = input_local_time.atZone(fromZone);

			ZoneId toZone = ZoneId.of(toTimeZoneId);
			ZonedDateTime outputZonedDateTime = inputZonedDateTime.withZoneSameInstant(toZone);
			return outputZonedDateTime;
		} catch (Exception ex) {
			throw ex;
		}
	}

	public static ZonedDateTime timeZoneConvertionFromString(String inputDate, String fromTimeZoneId,
			String toTimeZoneId) throws Exception {
		try {
			ZoneId fromZone = ZoneId.of(fromTimeZoneId);
			LocalDateTime input_local_time = LocalDateTime.ofInstant(TIME_FORMATTER.parse(inputDate).toInstant(),
					fromZone);
			ZonedDateTime inputZonedDateTime = input_local_time.atZone(fromZone);

			ZoneId toZone = ZoneId.of(toTimeZoneId);
			ZonedDateTime outputZonedDateTime = inputZonedDateTime.withZoneSameInstant(toZone);
			return outputZonedDateTime;
		} catch (Exception ex) {
			throw ex;
		}
	}

	public static Date convertLocalTimeToTimeZone(java.util.Date localTime, String fromTimeZone, String toTimeZone) {
		Date result = localTime;
		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
			LocalDateTime localtDateAndTime = LocalDateTime.parse(TIME_FORMATTER.format(localTime), formatter);
			ZonedDateTime dateAndTimeInLocal = ZonedDateTime.of(localtDateAndTime, ZoneId.of(fromTimeZone));
			ZonedDateTime toDateTime = dateAndTimeInLocal.withZoneSameInstant(ZoneId.of(toTimeZone));
			result = java.util.Date.from(toDateTime.toInstant());
		} catch (Exception ex) {
			LoggingUtilities
			.generateErrorLog("Failed to convertLocalTimeToTimeZone" );
			ex.printStackTrace();
		}
		return result;
	}
	
	public static String getCurrentDateTime_yyyyMMddhhmmss() {
		SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_YYYYMMDDHHmmSS);
		return dateFormat.format(new Date());
	}
	
	public static String getDateStringFromTimeStamp(Timestamp ts, String format) {
		if(ts == null) {
			return StringUtils.EMPTY;
		}
		try {
		return DateTimeFormatter.ofPattern(format).format(Instant.ofEpochMilli(ts.getTime()).atZone(ZoneId.systemDefault()));
		}catch (Exception e) {
			LoggingUtilities
			.generateErrorLog("Failed to execute getDateStringFromTimeStamp");
			e.printStackTrace();
			return null;
		}
	}
	
	public static Timestamp addHoursToTimeStamp(Timestamp timestamp, int hours) {
		if (timestamp == null) {
			return null;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timestamp);
		calendar.add(Calendar.HOUR, hours);
		return new Timestamp(calendar.getTimeInMillis());
	}

	public static Timestamp getTimestampFromString(String date) {
		if(StringUtils.isEmpty(date)) {
			return null;
		}
		try {
			return Timestamp.valueOf(date);
		}catch (Exception e) {
			LoggingUtilities
			.generateErrorLog("Failed to parse timeStampString : " + date);
			e.printStackTrace();
			return null;
		}
	}
	
	public static Date getDateFormatFromString(String date) {
		if(StringUtils.isEmpty(date)) {
			return null;
		}
		SimpleDateFormat formatOfDate = new SimpleDateFormat("MM/dd/yyyy"); //declare string as constant
		try {
			Date formattedDate = formatOfDate.parse(date);
			return formattedDate;
			
		}catch (Exception e) {
			LoggingUtilities
			.generateErrorLog("Failed to parse dateString : " + date);
			e.printStackTrace();
			return null;
		}
	}
	
	public static String getDateStringFromDate(Date date, String format) {
		if(date == null) {
			return StringUtils.EMPTY;
		}
		try {
		SimpleDateFormat df = new SimpleDateFormat(format);	
		return df.format(date);
		}catch (Exception e) {
			LoggingUtilities
			.generateErrorLog("Failed to execute getDateStringFromDate");
			e.printStackTrace();
			return null;
		}
	}
	
}
