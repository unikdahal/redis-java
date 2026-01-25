package com.redis.commands;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class RPushCommand implements ICommand{

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {

        return "";
    }

    @Override
    public String name() {
        return "RPUSH";
    }
}
