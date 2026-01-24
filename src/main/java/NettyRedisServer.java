import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyRedisServer {

    private final int port;

    public NettyRedisServer(int port) {
        this.port = port;
    }

    public void run() throws Exception{

        /*
            We have the concept of Boss Group and Worker Group in Netty.
            Boss Group is used for the main event loop here we are gonna use it for accepting connections,
            so we just need 1 thread for it
         */
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);

        /*
            For the Worker Group, it is used for handling the traffic of the accepted connections,
            so we can use more threads here, by default, it uses 2 * numbers of available processors.
            But for now since we are trying to achieve the functionality of Redis, we will only be using one thread
            CRITICAL: We set this to '1' to enforce the Single-Threaded Redis Model.
            This means we don't need locks for our data store later!
        * */

        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        try{
            /*
            ServerBootstrap is the entry point for setting up a Netty server.
            This configures the NioServerSocketChannel so that it accepts incoming TCP connections.
            We also set the ChannelInitializer, which is a special handler that is purposed to help configure a new Channel.
            Here we can add more handlers to the pipeline of the newly created channels.
            In our case we are adding the RedisCommandHandler to handle the incoming commands.
            * */
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new RedisCommandHandler());
                        }
                    });

            System.out.println("Starting Redis Server on port: " + port);

            //Binding and starting to accept the incoming connections
            ChannelFuture f = b.bind(port).sync();
            //wait until the server socket is closed
            f.channel().closeFuture().sync();
        }finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception{
        new NettyRedisServer(6379).run();
    }



}