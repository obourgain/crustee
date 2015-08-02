package org.crustee.raft.storage.btree;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

public class ArrayStackTest {

    @Test
    public void should_push() throws Exception {
        ArrayStack<String> stack = new ArrayStack<>(10);
        stack.push("foo");
        assertThat(stack.array).has(Utils.numberOfElementsAndNulls(1));
        assertThat(stack.array[0]).isEqualTo("foo");
        assertThat(stack.nextIndex).isEqualTo(1);
    }

    @Test
    public void should_pop() throws Exception {
        ArrayStack<String> stack = new ArrayStack<>(10);
        stack.push("foo");

        Object value = stack.pop();
        assertThat(value).isEqualTo("foo");
        assertThat(stack.array).has(Utils.numberOfElementsAndNulls(0));
        assertThat(stack.nextIndex).isEqualTo(0);
    }

    @Test
    public void should_throw_when_popping_empty_stack() throws Exception {
        ArrayStack<String> stack = new ArrayStack<>(10);
        Throwable throwable = ThrowableAssert.catchThrowable(stack::pop);
        assertThat(throwable).isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    public void should_resize_when_pushing_on_full_stack() throws Exception {
        ArrayStack<String> stack = new ArrayStack<>(1);
        stack.push("foo");
        stack.push("foo");

        assertThat(stack.nextIndex).isEqualTo(2);
    }
}