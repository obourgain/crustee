package org.crustee.raft;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Command {

    String key;
    String value;

}
