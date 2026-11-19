package scala.collection;

/** Test stand-in for Scala's {@code Iterator}; see {@link Seq}. */
public interface Iterator<A> {

    boolean hasNext();

    A next();
}
