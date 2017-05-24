
package com.github.venkateshamurthy.querykv;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * A query interface providing a base for broad range of queries that typically involve
 * querying objects having K key and its attributes as Map&lt;String,Object&gt; that can be
 * converted to a resultant object type of R.
 *
 * <p>Note: The methods that involve getting key(s) or object(s) typically need to make calls into
 * stream generating calls thus strongly support late reduction of stream to concrete types.
 * This is as per best practice of stream handling.
 *
 * @param <K> the main key of the record
 * @param <R> the desired result record mapping object
 */
public interface QueryInterface<K, R> {
   /** A constant predicate to say any entry is ok of a map's entry set. */
   Predicate<Entry<String, Object>> anyEntryOk = e -> true;

   /**
    * A predicate of type Entry&lt;String, Object&gt; to be transformed to be used in a map of type
    * Map&lt;K, Map&lt;String, Object&gt;&gt;.
    */
   Function<Predicate<Entry<String, Object>>, Predicate<Entry<?, Map<String, Object>>>>
      singleFieldMatcher = p -> {
         Preconditions.checkNotNull(p);
         return e -> e.getValue() != null && e.getValue().entrySet().stream().anyMatch(p);
      };

   /**
    * A predicate transformer.
    */
   Function<Predicate<Entry<String, Object>>, Predicate<Entry<String, Object>>> fieldPredicate = p -> {
      Preconditions.checkNotNull(p);
      return e -> e != null && p == anyEntryOk || p.test(e);
   };

   /**
    * An empty set to indicate all keys to be considered.
    *
    * @param <K> type of iterable infered statically
    * @return Iterable
    */
   static <K> Iterable<K> allkeys() {
      return Collections.<K> emptySet();
   }

   /**
    * Checks if key (i.e object having this key) exists and thus valid.
    *
    * @param key the key to be tested
    * @return true, if object exists in the repository
    */
   boolean exists(K key);


   /**
    * A getter for the ObjectMaker instance.
    *
    * @return {@link ResultObjectMaker}
    */
   ResultObjectMaker<K, R> objectMaker();

   /**
    * An object to key reverse mapper.
    *
    * @return a function to return a key from an object of restult type R.
    */
   Function<R, K> objectToKey();

   /**
    * A method to get the default projection attributes of the result object.
    *
    * @return an iterable of string names with each name corresponding to projection attribute name
    */
   Iterable<String> projectionAttributes();

   /**
    * queryForDetails provides a base function to query the inventory
    * repository using the set of keys (of type K), a projection attributes and with a result
    * mapper.
    *
    * @param keys a set of keys of type K or can be {@link #allkeys()}
    * @param attributes are the attribute names
    * @param resultMapper is a mapper to transform the result map to an object of type T
    * @param <T> the type of result that can be a map or typically a collection of descendant of
    *        some base object of type R.
    *
    * @return object of result type T
    */
   <T> T queryForDetails(Iterable<K> keys, Iterable<String> attributes,
            ResultMapper<K, T> resultMapper);


   /**
    * Query and make a object given a key and properties.
    *
    * @param key the key of the object
    * @return the instance of an object having this key or null.
    */
   default Optional<R> queryForObject(K key) {
      Preconditions.checkNotNull(key, "object reference key is missing!/null");
      return queryForObjects(Arrays.asList(key), e -> true).stream().findFirst();
   }

   /**
    * Query for objects of type R filtered on a predicate.
    *
    * @param keys the objects keys is a key set to which the repository is queried upon
    * @param predicate is the filter to apply upon to return the selected object
    * @return list of objects (which may or may not be emoty)
    */
   default List<R> queryForObjects(Iterable<K> keys, Predicate<R> predicate) {
      return queryForObjectStream(keys, predicate).collect(Collectors.toList());
   }

   /**
    * Query for Object stream of type R filtered on predicate. This method could prove useful when
    * the returned stream can be further used for additional transformation and filtering.
    *
    * @param keys the objects keys is a key set to which the repository is queried upon
    * @param predicate is the filter to apply upon to return the selected object
    * @return {@link Stream} of objects
    */
   default Stream<R> queryForObjectStream(Iterable<K> keys, Predicate<R> predicate) {
      return queryForEntryStream(keys, anyEntryOk, projectionAttributes())
               .map(objectMaker()).filter(predicate);
   }

   /**
    * Query to get an stream of object data in the form of {@link Entry} of object key and its
    * property map.
    *
    * @param keys the key set
    * @param predicate the filter on entries. If e->true is used all entries are selected.
    * @param attributes properties of projection
    * @return {@link Stream} of entries of type Entry&lt;K, Map&lt;String, Object&gt;&gt;
    */
   default Stream<Entry<K, Map<String, Object>>> queryForEntryStream(Iterable<K> keys,
            Predicate<Entry<String, Object>> predicate, Iterable<String> attributes) {
      Preconditions.checkNotNull(keys);
      Preconditions.checkNotNull(predicate);
      Preconditions.checkNotNull(attributes);
      return queryForDetails(keys, attributes, Predicates::entryStream)
               .filter(singleFieldMatcher.apply(predicate));
   }

   /**
    * Query for a key satisfying an object(of type R) predicate.
    *
    * @param predicate a Predicate&ltR&gt;
    * @return key
    */
   default Optional<K> queryForKey(Predicate<R> predicate) {
      Optional<R> result = queryForObjects(QueryInterface.allkeys(), predicate).stream().findFirst();
      return Optional.ofNullable(result.isPresent() ? objectToKey().apply(result.get()) : null);
   }

   /**
    * Query for a key satisfying an object(of type R) predicate.
    *
    * @param predicate a Predicate&ltR&gt;
    * @return key
    */
   default List<K> queryForKeys(Predicate<R> predicate) {
      return queryForObjects(QueryInterface.allkeys(), predicate).stream()
               .map(objectToKey()).collect(Collectors.toList());
   }

   /**
    * An interface for mapping the query result of the form Map&lt;K, Map&lt;String,Object&gt;&gt;
    * to a specific result type T.
    *
    * @param <K> the type of the key
    * @param <T> the property map to be modeled/mapped to any object of type T. This can be a
    *        stream, map or an object and is versatile compared to {@link ResultObjectMaker}.
    */
   interface ResultMapper<K, T> extends Function<Map<K, Map<String, Object>>, T> {
   }

   /**
    * A result object creator to create an object of type R given an entry of its key of type K and property map
    * of the type Map&lt;String,Object&gt;.
    *
    * @param <K> the type of the key
    * @param <R> the object of type R to map for.
    */
   interface ResultObjectMaker<K, R> extends Function<Entry<K, Map<String, Object>>, R> {
   }
}