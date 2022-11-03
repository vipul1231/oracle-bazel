package com.example.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// TODO consider making StateHistory a decorator for method calls like DataUpdater#update
public class StateHistory<State> {

    private final Class<State> stateClass;
    private final Map<StateEvent, State> chronologicalHistory;
//    private final ObjectMapper mapper;

    public enum StateEvent {
        BeforeSync,
        AfterSync
    }

    public StateHistory(Class<State> stateClass) {
        this.stateClass = stateClass;
        this.chronologicalHistory = new HashMap<>();
//        this.mapper = DefaultObjectMapper.JSON;
    }

    public void addEvent(StateEvent stateEvent, State state) {
        chronologicalHistory.put(stateEvent, deepCopy(state));
    }

    public State getEvent(StateEvent stateEvent) {
        State state =
                Optional.of(chronologicalHistory.get(stateEvent)).get();
        return deepCopy(state);
    }

    private State deepCopy(State state) {
//        try {
//            return mapper.treeToValue(mapper.valueToTree(state), stateClass);
            return null;
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
    }
}
