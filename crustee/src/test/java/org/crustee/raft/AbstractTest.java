package org.crustee.raft;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.crustee.raft.rpc.Rpc;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractTest {

    static final Clock DEFAULT_CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    @Mock
    protected MessageBus messageBus;

    protected List<Rpc> messages = new ArrayList<>();

    @Before
    public void capture_messages() {
        doAnswer(invocation -> {
            messages.add((Rpc) invocation.getArguments()[1]);
            return null;
        }).when(messageBus).send(any(), any());
    }

}
