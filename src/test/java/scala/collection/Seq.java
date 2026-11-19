package scala.collection;

import java.util.List;

/** Test stand-in for Scala's {@code Seq}, which Spark's {@code Row.get} returns for an array column. Not Scala. */
public interface Seq<A> {

    Iterator<A> iterator();

    static <A> Seq<A> of(List<A> elements) {
        return () -> {
            java.util.Iterator<A> javaIterator = elements.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return javaIterator.hasNext();
                }

                @Override
                public A next() {
                    return javaIterator.next();
                }
            };
        };
    }
}
