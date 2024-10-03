package com.andrei.mensageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumidor {
    private final static String QUEUE_NAME = "durable_persistent_queue";

    public static void main(String[] argv) throws Exception {
        // Configuração da conexão
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Criando conexão e canal fora do try-with-resources
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declaração da fila
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println("Aguardando mensagens na fila: " + QUEUE_NAME);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("Recebida: '" + message + "'");

            // Extrai o timestamp do envio da mensagem
            String[] parts = message.split(" enviada em ");
            long sentTimestamp = Long.parseLong(parts[1]);

            // Calcula a diferença
            long receivedTimestamp = System.currentTimeMillis();
            long difference = receivedTimestamp - sentTimestamp;

            // Imprime a diferença
            System.out.println("Diferença de tempo: " + difference + " ms");

            // Acknowledgment manual após processamento
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };



        // Configura o consumidor para não fazer ack automático
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });

        // Adiciona uma linha para manter o programa em execução
        // Isso mantém o consumidor ativo até que seja interrompido manualmente
        System.out.println("Pressione [enter] para sair.");
        System.in.read();

        // Fechar o canal e a conexão ao sair
        channel.close();
        connection.close();
    }
}
