package org.crustee.raft.bootstrap;

import static org.slf4j.LoggerFactory.getLogger;
import org.slf4j.Logger;

public class Crustee {

    private static final Logger logger = getLogger(Crustee.class);

    public static void main(String[] args) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.start();
    }

}
