package com.andrei.mensageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ConsumidorDiferença {
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

        // Classe auxiliar para armazenar os timestamps
        class TimestampHolder {
            Long firstTimestamp = null;
            Long lastTimestamp = null;
        }

        TimestampHolder timestamps = new TimestampHolder();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");


            // Extrai o timestamp do envio da mensagem
            String[] parts = message.split(" enviada em ");
            long sentTimestamp = Long.parseLong(parts[1]);

            // Atualiza o primeiro e o último timestamp
            if (timestamps.firstTimestamp == null) {
                timestamps.firstTimestamp = sentTimestamp; // Armazena o timestamp da primeira mensagem
                System.out.println("Primeira mensagem recebida com timestamp: " + timestamps.firstTimestamp);
            }
            timestamps.lastTimestamp = sentTimestamp; // Atualiza o timestamp da última mensagem

            // Acknowledgment manual após processamento
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };


        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });


        System.out.println("Consumidor em execução. Pressione [Ctrl+C] para sair.");
        while (true) {
            // Aguarda um pequeno tempo para não sobrecarregar a CPU
            Thread.sleep(1000); // 1 segundo

            // Calcular e imprimir a diferença se a última mensagem foi recebida
            if (timestamps.lastTimestamp != null && timestamps.firstTimestamp != null) {
                long difference = timestamps.lastTimestamp - timestamps.firstTimestamp;

                // Imprime a diferença
                System.out.println("Diferença entre a primeira e a última mensagem: " + difference + " ms");
            }
        }

        // Fechar o canal e a conexão ao sair (nunca será alcançado neste loop)
        // channel.close();
        // connection.close();
    }
}
