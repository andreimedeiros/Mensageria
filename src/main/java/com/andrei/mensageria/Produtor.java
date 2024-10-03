package com.andrei.mensageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * Classe responsavel por enviar itens à fila
 */
public class Produtor {

    private final static String QUEUE_NAME = "durable_persistent_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            // Declaração da fila durável
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            System.out.println("Fila durável criada: " + QUEUE_NAME);

            for (int i = 1; i <= 1000000; i++) {
                String message = "Mensagem " + i + " enviada em " + System.currentTimeMillis();
                // Enviando mensagem não persistente
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                System.out.println("Enviada: '" + message + "'");
            }
        }
    }
}

