package Netty.TestDemo;/**
 * @description
 * @author: WuYe
 * @vesion:1.0
 * @Data : 2020/11/19 16:50
 */

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;

/**
 * @program: BD_DAQ_InputSplit
 *
 * @description:
 *
 * @author: WuYe
 *
 * @create: 2020-11-19 16:50
 **/
public class ClientHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        System.out.println("ClientHandler.channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] bytes = new byte[result.readableBytes()];
        result.readBytes(bytes);
        result.release();
        ctx.close();
        System.out.println("server said:"+new String(bytes));
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("ClientHandler.channelActive");
        String msg="Are you ok";
        ByteBuf encode = ctx.alloc().buffer(4 * msg.length());
        encode.writeBytes(msg.getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(encode);
    }
}
