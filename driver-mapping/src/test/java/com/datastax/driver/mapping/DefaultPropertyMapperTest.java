/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.mapping.annotations.Column;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * The mapper derives both CQL column names and Java accessor names by folding case, so neither may
 * depend on the JVM's default locale. In a Turkish locale an upper-case I folds to a dotless {@code
 * ı} and a lower-case i to a dotted {@code İ}, which is exactly the letter that broke CUSTOMER-583
 * one layer up in {@code ColumnDefinitions}.
 */
public class DefaultPropertyMapperTest {

  private static final Locale TURKISH = new Locale("tr", "TR");

  static class Entity {
    @Column(name = "ID")
    int id;
  }

  /** A property whose setter is "relaxed" (it returns the entity rather than void). */
  static class RelaxedSetterEntity {
    private int id;

    public int getId() {
      return id;
    }

    public RelaxedSetterEntity setId(int id) {
      this.id = id;
      return this;
    }
  }

  /**
   * An explicit, non-case-sensitive {@code @Column(name = "ID")} is folded to the name the server
   * holds. Under a Turkish default locale an unpinned fold yields a dotless id, and every read and
   * write of the property would then miss the column.
   */
  @Test(groups = "unit")
  public void should_fold_explicit_column_name_in_any_default_locale() throws Exception {
    Field field = Entity.class.getDeclaredField("id");
    Map<Class<? extends Annotation>, Annotation> annotations =
        Collections.<Class<? extends Annotation>, Annotation>singletonMap(
            Column.class, field.getAnnotation(Column.class));

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(TURKISH);
      String mappedName =
          new DefaultPropertyMapper().inferMappedName(Entity.class, "id", annotations);
      assertThat(mappedName).isEqualTo("id");
    } finally {
      Locale.setDefault(def);
    }
  }

  /**
   * The relaxed-setter lookup builds a Java method name from the property name. Under a Turkish
   * default locale an unpinned fold looks for setId spelled with a dotted I; the resulting
   * NoSuchMethodException is swallowed, so the setter would go silently undetected rather than
   * failing loudly.
   */
  @Test(groups = "unit")
  public void should_locate_relaxed_setter_in_any_default_locale() throws Exception {
    PropertyDescriptor descriptor = null;
    for (PropertyDescriptor candidate :
        Introspector.getBeanInfo(RelaxedSetterEntity.class).getPropertyDescriptors()) {
      if ("id".equals(candidate.getName())) {
        descriptor = candidate;
      }
    }
    assertThat(descriptor).isNotNull();
    // Precondition for the branch under test: a relaxed setter is not a bean write method.
    assertThat(descriptor.getWriteMethod()).isNull();

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(TURKISH);
      Method setter =
          new DefaultPropertyMapper().locateSetter(RelaxedSetterEntity.class, descriptor);
      assertThat(setter).isNotNull();
      assertThat(setter.getName()).isEqualTo("setId");
    } finally {
      Locale.setDefault(def);
    }
  }
}
