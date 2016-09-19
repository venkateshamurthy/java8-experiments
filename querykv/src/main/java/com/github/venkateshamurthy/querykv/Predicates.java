package com.github.venkateshamurthy.querykv;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * A set of predicates.
 */
public final class Predicates {

   /** Constructor. */
   private Predicates() {
      throw new UnsupportedOperationException(
    		  this.getClass().getSimpleName() + " class should not to be constructed!");
   }

   /**
    * A supplier to contain a given collection that could be used in a Stream Collector.
    *
    * @param collection to be contined within a supplier.
    * @param <C> collection type
    * @param <T> the object type that collection holds.
    * @return Supplier having a collection within.
    */
   public static <T, C extends Collection<T>> Supplier<C> withinSupplier(C collection) {
      return () -> collection;
   }

   /**
    * Collect the data to a supplied collecttion.
    *
    * @param collection can be one of List, Set typically.
    * @param <T> type of data
    * @param <C> type of collection
    * @return a Collector possessing the Collection passed wrapped in a Supplier.
    */
   public static <T, C extends Collection<T>> Collector<T, ?, C> toSuppliedCollection(C collection) {
      return Collectors.toCollection(withinSupplier(collection));
   }

   /**
    * Stream of keys in a map.
    *
    * @param map whose keys to be streamed
    * @param <K> type of key
    * @param <V> type of value
    * @return stream of key
    */
   public static <K, V> Stream<K> keyStream(Map<K, V> map) {
      return map.keySet().stream();
   }

   /**
    * Stream of keys in a collection of Map.Entry.
    *
    * @param collection whose keys to be streamed
    * @param <K> type of key
    * @param <V> type of value
    * @return stream of key
    */

   public static <K, V> Stream<K> keyStream(Collection<Entry<K, V>> collection) {
      return collection.stream().map(Entry::getKey);
   }

   /**
    * Stream of entries in a map.
    *
    * @param map whose keys to be streamed
    * @param <K> type of key
    * @param <V> type of value
    * @return stream of entries
    */

   public static <K, V> Stream<Entry<K, V>> entryStream(Map<K, V> map) {
      return map.entrySet().stream();
   }

   /**
    * Create entry.
    *
    * @param key of the entry.
    * @param value of the entry.
    * @param <K> type of key
    * @param <V> type of value
    * @return SimpleEntry.
    */
   public static <K, V> Map.Entry<K, V> entry(K key, V value) {
      return new SimpleEntry<>(key, value);
   }

   /**
    * Entries to map.
    *
    * @param <K> type of key.
    * @param <U> type of value.
    * @return Collector that can map entries to a map.
    */
   public static <K, U> Collector<Entry<K, U>, ?, Map<K, U>> entriesToMap() {
      return entriesToMap(HashMap::new);
   }

   /**
    * Entries to bimap.
    *
    * @param <K> type of key.
    * @param <U> type of value.
    * @return Collector that can map entries to a bimap.
    */
   public static <K, U> Collector<Entry<K, U>, ?, BiMap<K, U>> entriesToBiMap() {
      return entriesToMap(HashBiMap::create);
   }

   /**
    * Entries to linked hash map.
    *
    * @param <K> type of key.
    * @param <U> type of value.
    * @return Collector that can map entries to a linked hash map.
    */
   public static <K, U> Collector<Entry<K, U>, ?, LinkedHashMap<K, U>> entriesToLinkedMap() {
      return entriesToMap(LinkedHashMap::new);
   }

   /**
    * Entries to supplier of a map.
    *
    * @param mapSupplier a supplier of the map that is used for collecting entries.
    * @param <K> type of key.
    * @param <U> type of value.
    * @param <M> type of map.
    * @return Collector that can map entries through a supplier of map.
    */
   public static <K, U, M extends Map<K, U>> Collector<Entry<K, U>, ?, M> entriesToMap(
            Supplier<M> mapSupplier) {
      return Collectors.toMap(e -> e.getKey(), e -> e.getValue(), throwingMerger(), mapSupplier);
   }

   /**
    * Mapping entry keys and values.
    *
    * @param keyTransform to transform key from K1 to K2
    * @param valueTransform to transform value from V1 to V2.
    * @param <K1> type of to be transformed key
    * @param <K2> type of transformed key desired
    * @param <V1> type of to be transformed value
    * @param <V2> type of transformed value.
    * @return Function that can effect the key and value mapping.
    */
   public static <K1, V1, K2, V2> Function<Entry<K1, V1>, Entry<K2, V2>> entryMapper(
            Function<K1, K2> keyTransform, Function<V1, V2> valueTransform) {
      return e -> entry(keyTransform.apply(e.getKey()), valueTransform.apply(e.getValue()));
   }

   /**
    * A key transformer for the map entry.
    *
    * @param keyTransform that can be applied to the key of the entry
    * @param <K1> type of to be transformed key
    * @param <K2> type of transformed key desired
    * @param <V> type of value not to be transformed
    * @return a function that creates another entry with key being transformed
    */
   public static <K1, K2, V> Function<Entry<K1, V>, Entry<K2, V>> keyMapper(
            Function<K1, K2> keyTransform) {
      return e -> entry(keyTransform.apply(e.getKey()), e.getValue());
   }

   /**
    * A value transformer for the map entry.
    *
    * @param valueTransform that can be applied to the key of the entry
    * @param <K> type of the key not altered/transformed
    * @param <V1> type of value to be desired for transformation
    * @param <V2> type of transformed value
    * @return a function that creates another entry with value being transformed
    */
   public static <K, V1, V2> Function<Entry<K, V1>, Entry<K, V2>> valueMapper(
            Function<V1, V2> valueTransform) {
      return e -> entry(e.getKey(), valueTransform.apply(e.getValue()));
   }

   /**
    * Returning predicate which evaluates to always true.
    *
    * @param <T> a type to be narrowed down to
    * @return predicate
    */
   public static <T> Predicate<T> everTrue() {
      return ConstantPredicate.ETERNALLY_TRUE.typify();
   }

   /**
    * Return predicate which evaluates to true if object is null.
    *
    * @param <T> a type to be narrowed down to
    * @return predicate
    */
   public static <T> Predicate<T> isNull() {
      return ConstantPredicate.IS_NULL.typify();
   }

   /**
    * Return predicate which evaluates to true if object is not null.
    *
    * @param <T> a type to be narrowed down to
    * @return predicate
    */
   public static <T> Predicate<T> isNotNull() {
      return ConstantPredicate.IS_NOT_NULL.typify();
   }

   /**
    * Predicate which evaluates to true if entry value is null.
    *
    * @param <K> key type of an entry
    *
    * @param <V> value type of an entry
    * @return predicate
    */
   public static <K, V> Predicate<Entry<K, V>> isEntryValueNull() {
      return e -> isNotNull().test(e) && isNull().test(e.getValue());
   }

   /** An inner enum to encapsulate the actual predicate which cant be typified(or typed). */
   private enum ConstantPredicate implements Predicate<Object> {
      /** enum predicate for eternally true. */
      ETERNALLY_TRUE {
         @Override
         public boolean test(Object object) {
            return true;
         }
      },

      /** enum predicate for eternally false. */
      ETERNALLY_FALSE {
         @Override
         public boolean test(Object object) {
            return false;
         }
      },

      /** enum predicate for checking object null state. */
      IS_NULL {
         @Override
         public boolean test(Object object) {
            return Objects.isNull(object);
         }
      },

      /** enum predicate for checking object is not null. */
      IS_NOT_NULL {
         @Override
         public boolean test(Object object) {
            return object != null;
         }
      };

      @SuppressWarnings("unchecked")
      <T> Predicate<T> typify() {
         return (Predicate<T>) this;
      }
   }

   /** A merging action to throw. */
   private static <T> BinaryOperator<T> throwingMerger() {
      return (uarg, varg) -> {
         throw new IllegalStateException(String.format("Duplicate key %s", uarg));
      };
   }
}