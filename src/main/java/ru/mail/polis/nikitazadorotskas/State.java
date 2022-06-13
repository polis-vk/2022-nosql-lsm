package ru.mail.polis.nikitazadorotskas;

class State {
    final Memory memory;
    final Memory flushingMemory;
    final Storage storage;

    State(Memory memory, Memory flushingMemory, Storage storage) {
        this.memory = memory;
        this.flushingMemory = flushingMemory;
        this.storage = storage;
    }

    State prepareForFlush() {
        return new State(new Memory(), memory, storage);
    }

    State finishFlush() {
        return new State(memory, null, storage);
    }
}
