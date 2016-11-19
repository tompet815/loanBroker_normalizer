package utilities;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import models.Data;
import models.XMLData;


public class MessageUtility {

    public Object deSerializeBody(byte[] body) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(body);
        ObjectInput in = new ObjectInputStream(bis);
        return in.readObject();
    }

    public byte[] serializeBody(Object obj) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        byte[] res = bos.toByteArray();
        out.close();
        bos.close();
        return res;
    }
    
      public XMLData convertToXMLData(Data data)  {
          Calendar today = Calendar.getInstance(); 
          int addedYear = 1970+data.getLoanDuration();
          today.set(addedYear,0, 1);
          Date durationDate=today.getTime();
          SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");      


        return new XMLData(data.getSsn(),data.getCreditScore(),data.getLoanAmount(),  df.format(durationDate));
    }

}
