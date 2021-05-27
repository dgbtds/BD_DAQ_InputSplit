package Netty.TestDemo;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/19 16:44
 */

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.UnsupportedEncodingException;

/**
 * @program: BD_DAQ_InputSplit
 * @description:
 * @author: WuYe
 * @create: 2020-11-19 16:44
 **/
public class ClientNetty {

    // 要请求的服务器的ip地址
    private String ip;
    // 服务器的端口
    private int port;

    public ClientNetty(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    // 请求端主题
    private void action() {

        EventLoopGroup workGroup = new NioEventLoopGroup();

        Bootstrap bs = new Bootstrap();
        ChannelFuture cf = null;
        try {
            bs.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {

                            // 处理来自服务端的响应信息
                            socketChannel.pipeline().addLast(new ClientHandler());
                            socketChannel.pipeline().addLast(new OutboundHandlerA());
                        }
                    });

            // 客户端开启
             cf = bs.connect(ip, port).sync();

            // 等待直到连接中断
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (cf!=null) {
                cf.channel().close();
            }
            workGroup.shutdownGracefully();
        }

    }

    public static void main(String[] args) {
        new ClientNetty("127.0.0.1", 20000).action();
    }

}
