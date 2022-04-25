package com.lcl.scs.util.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingUtilities {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

	public static String getStackTrace(Exception exception) {
		StringWriter errors = new StringWriter();
		exception.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}

	protected static Logger getLogger() {
		return logger;
	}

	/*public static void generateValidationLog(Exception exception, String message, String validationDetails, String actionBy, String api) {
		logger.info(ScvUtilities.appendNewLines("API: " + api, message, "Validation Details " + validationDetails, "Action By " + actionBy));
	}
	
	public static void generateValidationLog(Exception exception, String message, String validationDetails, String api) {
		logger.info(ScvUtilities.appendNewLines("API: " + api, message, "Validation Details " + validationDetails));
	}*/

	public static void generateErrorLog(Exception exception, String actionBy, String transactionId) {
		getLogger().error(generateMessage(exception == null ? null : exception.getMessage(), actionBy, transactionId), exception);
	}

	public static void generateErrorLog(String message, String actionBy, String transactionId) {
		getLogger().error(generateMessage(message, actionBy, transactionId));
	}

	public static void generateErrorLog(String message) {
		getLogger().error(generateMessage(message));
	}

	public static void generateInfoLog(String message, String actionBy, String transactionId) {
		getLogger().info(generateMessage(message, actionBy, transactionId));
	}

	public static void generateInfoLog(String message) {
		getLogger().info(generateMessage(message));
	}

	private static String generateMessage(String message, String actionBy, String transactionId) {
		if (actionBy != null) {
			return new StringBuffer().append("Message:").append(message).append(", actionBy:").append(actionBy).append(", transactionId:").append(transactionId).toString();
		} else {
			return new StringBuffer().append("Message:").append(message).toString();
		}
	}

	private static String generateMessage(String message) {
			return new StringBuffer().append("Message:").append(message).toString();
	}

	public static void generateDebugLog(String message, String actionBy, String transactionId) {
		getLogger().debug(generateMessage(message, actionBy, transactionId));
	}

	public static void generateWarningLog(String message, String actionBy, String transactionId) {
		getLogger().warn(generateMessage(message, actionBy, transactionId));
	}

}
