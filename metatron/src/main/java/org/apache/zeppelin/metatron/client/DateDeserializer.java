package org.apache.zeppelin.metatron.client;

import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateDeserializer implements JsonDeserializer<Date> {
  SimpleDateFormat formatter;

  public DateDeserializer() {
    formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Override
  public Date deserialize(JsonElement element, Type arg1, JsonDeserializationContext arg2) throws JsonParseException {
    String date = element.getAsString();

    try {
      return formatter.parse(date);
    } catch (ParseException e) {
      throw new JsonParseException(e);
    }
  }
}
