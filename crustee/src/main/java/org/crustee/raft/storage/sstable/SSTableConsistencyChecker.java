package org.crustee.raft.storage.sstable;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.READ;
import static org.crustee.raft.utils.ByteBufferUtils.toHexString;
import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.position;
import static org.crustee.raft.utils.UncheckedIOUtils.size;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.crustee.raft.utils.UncheckedIOUtils;

public class SSTableConsistencyChecker {

    private final Consumer<String> inconsistencyAction;

    private FileChannel tableChannel;
    private FileChannel indexChannel;

    public SSTableConsistencyChecker(Path table, Path index, Consumer<String> inconsistencyAction) {
        this.inconsistencyAction = inconsistencyAction;
        this.tableChannel = openChannel(table, READ);
        this.indexChannel = openChannel(index, READ);
    }

    public void check() {
        SSTableHeader header = checkHeader(tableChannel);
        verify(() -> position(tableChannel) == SSTableHeader.BUFFER_SIZE, () -> format("position should be at the header length: %s , got %s", SSTableHeader.BUFFER_SIZE, position(tableChannel)));
        checkEntries(header);
    }

    private SSTableHeader checkHeader(FileChannel tableChannel) {
        ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.BUFFER_SIZE);
        UncheckedIOUtils.read(tableChannel, buffer);
        SSTableHeader header = SSTableHeader.fromBuffer(buffer);
        verify(() -> header.getEntryCount() >= 0, () -> format("header entry count is negative %s", header.getEntryCount()));
        long tableFileSize = size(tableChannel);
        verify(() -> header.getSize() == tableFileSize, () -> format("size recorded in header %s is different from file size %s ", header.getSize(), tableFileSize));
        return header;
    }

    private void checkEntries(SSTableHeader header) {
        ByteBuffer tableEntryMetadata = ByteBuffer.allocate(2 + 4); // enough for reading keysize + valuesize as short + int
        ByteBuffer indexEntryMetadata = ByteBuffer.allocate(2 + 8 + 4); // keysize (short) + offset (long) + value size (int)
        ByteBuffer tableKeyBuffer = ByteBuffer.allocate(100); // arbitrary value, will be replaced if needed
        ByteBuffer indexKeyBuffer = ByteBuffer.allocate(100);
        ByteBuffer valueBuffer = ByteBuffer.allocate(100);

        Buffers buffers = new Buffers(tableEntryMetadata, indexEntryMetadata, tableKeyBuffer, indexKeyBuffer, valueBuffer);

        for (int i = 0; i < header.getEntryCount(); i++) {
            checkEntry(buffers, i);
        }

        verify(() -> position(tableChannel) == size(tableChannel), () -> format("expected to be at the end of file, position is %s and size %s", position(tableChannel), size(tableChannel)));
    }

    private void checkEntry(Buffers buffers, int entryIndex) {
        ByteBuffer tableEntryMetadata = buffers.tableEntryMetadata();
        ByteBuffer indexEntryMetadata = buffers.indexEntryMetadata();

        UncheckedIOUtils.read(tableChannel, tableEntryMetadata);
        UncheckedIOUtils.read(indexChannel, indexEntryMetadata);

        tableEntryMetadata.flip();
        indexEntryMetadata.flip();
        short tableKeySize = tableEntryMetadata.getShort();
        int tableValueSize = tableEntryMetadata.getInt();
        short indexKeySize = indexEntryMetadata.getShort();
        long offset = indexEntryMetadata.getLong();
        int indexValueSize = indexEntryMetadata.getInt();

        verify(() -> tableKeySize == indexKeySize, () -> format("at entry %s, keys do not have the same length in table and index: %s / %s", entryIndex, tableKeySize, indexKeySize));
        verify(() -> tableValueSize == indexValueSize, () -> format("at entry %s, columns do not have the same length in table and index: %s / %s", entryIndex, tableKeySize, indexKeySize));

        ByteBuffer tableKeyBuffer = buffers.tableKeyBuffer(tableKeySize);
        ByteBuffer valueBuffer = buffers.valueBuffer(tableValueSize);
        ByteBuffer indexKeyBuffer = buffers.indexKeyBuffer(indexKeySize);

        UncheckedIOUtils.read(tableChannel, tableKeyBuffer);
        long currentValueOffset = position(tableChannel);
        UncheckedIOUtils.read(tableChannel, valueBuffer);
        UncheckedIOUtils.read(indexChannel, indexKeyBuffer);

        verify(() -> Objects.equals(tableKeyBuffer.rewind(), indexKeyBuffer.rewind()), () -> format("at entry %s, key buffers doesn't have the same content: table %s , index %s ", entryIndex, toHexString(tableKeyBuffer), toHexString(indexKeyBuffer)));
        verify(() -> currentValueOffset == offset, () -> format("at entry %s, index offset and table position are different: table %s , index %s ", entryIndex, currentValueOffset, offset));
    }

    private void verify(BooleanSupplier check, Supplier<String> message) {
        if (!check.getAsBoolean()) {
            inconsistencyAction.accept(message.get());
        }
    }

}
