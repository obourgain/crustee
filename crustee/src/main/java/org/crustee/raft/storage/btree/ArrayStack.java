package org.crustee.raft.storage.btree;

public class ArrayStack<T> {

    protected Object[] array;
    protected int nextIndex = 0;

    public ArrayStack() {
        this(10);
    }

    public ArrayStack(int size) {
        this.array = new Object[size];
    }

    public void push(T o) {
        if (nextIndex == array.length) {
            resize();
        }
        array[nextIndex] = o;
        nextIndex++;
    }

    private void resize() {
        int oldSize = array.length;
        int newSize = oldSize << 1; // double size
        Object[] newArray = new Object[newSize];
        System.arraycopy(array, 0, newArray, 0, array.length);
        array = newArray;
    }

    public T pop() {
        nextIndex--;
        Object o = array[nextIndex];
        array[nextIndex] = null; // really useful or just overwrite ?
        //noinspection unchecked
        return (T) o;
    }

    public T peek() {
        //noinspection unchecked
        return (T) array[nextIndex - 1];
    }

    public boolean isEmpty() {
        return nextIndex == 0;
    }

    public void clear() {
        nextIndex = 0;
        // release references to any nodes referenced here to free garbage
        Object[] array = this.array;
        for (int i = 0; i < array.length; i++) {
            Object o = array[i];
            if(o == null){
                return;
            }
            array[i] = null;
        }

    }
}
