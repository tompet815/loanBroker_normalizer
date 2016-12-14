package normalizer;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP.*;
import models.Data;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import connector.RabbitMQConnector;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import models.LoanResponse;
import utilities.MessageUtility;

public class Normalizer {

    private final RabbitMQConnector connector = new RabbitMQConnector();
    private Channel channel;
    private final String QUEUENAME = "whatNormalizerQueue";
    private final String EXCHANGENAME = "whatNormalizer";
    private final String AGGREGATOREXCHANGENAME = "whatAggrigator";
    private final MessageUtility util = new MessageUtility();

    //initialize Normalizer
    public void init() throws IOException {

        channel = connector.getChannel();
        channel.queueDeclare(QUEUENAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGENAME, "direct");
        channel.queueBind(QUEUENAME, EXCHANGENAME, "");
        receive();
    }

    //Waiting asynchronously for messages
    public boolean receive() throws IOException {

        System.out.println(" [*] Waiting for messages.");
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                System.out.println(" [x] Received ");
                try {
                  
                    String bodyString = removeBom(new String(body));
                    LoanResponse data = translate(bodyString);

                    send(properties, data);
                }
                catch (JAXBException ex) {
                    Logger.getLogger(Normalizer.class.getName()).log(Level.SEVERE, null, ex);
                }
                finally {
                    System.out.println(" [x] Done");
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume(QUEUENAME, false, consumer);
        return true;
    }

    private LoanResponse translate(String bodyString) {

        LoanResponse response = null;
        if (isXML(bodyString)) {
            try {
                response = unmarchal(bodyString);
            }
            catch (JAXBException ex) {
                Logger.getLogger(Normalizer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else {
            Gson gson = new Gson();
            response = gson.fromJson(bodyString, LoanResponse.class);
        }
        return response;
    }

    //unmarshal from string to Object
    private LoanResponse unmarchal(String bodyString) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(LoanResponse.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        StringReader reader = new StringReader(bodyString);
        return (LoanResponse) unmarshaller.unmarshal(reader);
    }

    //marshal from pbkect to xml string
    private String marchal(LoanResponse d) throws JAXBException {
        JAXBContext jc2 = JAXBContext.newInstance(LoanResponse.class);
        Marshaller marshaller = jc2.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        JAXBElement<LoanResponse> je2 = new JAXBElement(new QName("LoanResponse"), LoanResponse.class, d);
        StringWriter sw = new StringWriter();
        marshaller.marshal(je2, sw);

        return removeBom(sw.toString());
    }

    private boolean isXML(String str) {
        return str.startsWith("<LoanResponse>")|| str.startsWith("<?xml");

    }

    //remove unnecessary charactors before xml declaration 
    private String removeBom(String bodyString) {
        String res = bodyString.trim();
        int substringIndex = res.indexOf("<?xml");
        if (substringIndex < 0) {
            return res;
        }
        return res.substring(res.indexOf("<?xml"));
    }

    //build a new property for messaging
    private BasicProperties propBuilder(String corrId, Map<String, Object> headers) {
        BasicProperties.Builder builder = new BasicProperties.Builder();
        builder.correlationId(corrId);
        builder.headers(headers);
        BasicProperties prop = builder.build();
        return prop;
    }

    //send message to exchange
    public boolean send(BasicProperties prop, LoanResponse data) throws IOException, JAXBException {
        //creating data for sending
        //send message to each bank in the banklist. 
        String xmlString = marchal(data);
        byte[] body = util.serializeBody(xmlString);
        String corrId = prop.getCorrelationId();
        //information is added in corrId about how many message aggregator is going to receive for the corrId
        BasicProperties newProp = propBuilder(corrId, prop.getHeaders());
        System.out.println("sending"+ newProp.getHeaders().get("bankName")+" from normalizer to " + AGGREGATOREXCHANGENAME + " : " + xmlString);
        channel.basicPublish(AGGREGATOREXCHANGENAME, "", newProp, body);

        return true;
    }

}
