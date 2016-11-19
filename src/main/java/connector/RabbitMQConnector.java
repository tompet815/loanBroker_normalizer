package connector;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;

public class RabbitMQConnector {

    private ConnectionFactory confactory;
    private Connection connection;

    public Channel getChannel() throws IOException {
        confactory = new ConnectionFactory();
        confactory.setHost("datdb.cphbusiness.dk");
        confactory.setUsername("what");
        confactory.setPassword("what");
        connection = confactory.newConnection();
        return connection.createChannel();
    }

    public void close(Channel channel) throws IOException {
        channel.close();
        connection.close();
    }
}
