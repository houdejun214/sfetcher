package com.sdata.core;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.internal.util.AbstractFileClassLoader;
import scala.reflect.internal.util.BatchSourceFile;
import scala.reflect.io.AbstractFile;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;
import scala.tools.nsc.GenericRunnerSettings;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.settings.MutableSettings;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

public class FetcherInterpreter {

    public interface MustConform {
        int doIt();
    }
    private static class ErrorHandler extends AbstractFunction1<String, BoxedUnit> {
        @Override
        public BoxedUnit apply(String message) {
            System.err.println("Interpreter error: " + message);
            return BoxedUnit.UNIT;
        }
    }

    private static File createSource() throws IOException {
        File source = File.createTempFile("code-", ".scala");

        try (PrintWriter out = new PrintWriter(source)) {
            out.println("import eri.ScalaInterpreterExample.MustConform");
            out.println("case class FooBar(name: String, value: Int) extends MustConform {");
            out.println("    def doIt: Int = 34");
            out.println("}");
        }
        return source;
    }

    public static void main(String[] args) {

        try {
            // Setup the compiler/interpreter
            GenericRunnerSettings settings = new GenericRunnerSettings(new ErrorHandler());

            // In scala this is settings.usejavacp.value = true;
            // It it through this setting that the compiled code is able to reference the
            // `MustConform` interface. The runtime classpath leaks into the compiler classpath, but
            // we're OK with that in this use case.
            ((MutableSettings.BooleanSetting) settings.usejavacp()).v_$eq(true);
            IMain interpreter = new IMain(settings);

            // Create and prepare source to compile.
            File source = createSource();
            // Java->Scala conversion nastiness.
            Iterable<Object> sources = Collections.singletonList(new BatchSourceFile(AbstractFile.getFile(source.toString())));
            Seq seq = JavaConversions.asScalaIterator((java.util.Iterator<Object>) sources).toSeq();

            // Compile source file(s).
            interpreter.compileSources(seq);

            // Find out what was compiled.
            Collection<Class<? extends MustConform>> classes = compiledConformers(interpreter);

            for(Class<? extends MustConform> c : classes) {

                // Interface doesn't enforce a constructor signature. This is just to have it more
                // interesting than the default ctor.
                Constructor<? extends MustConform> ctor = c.getConstructor(String.class, int.class);

                MustConform instance = ctor.newInstance("Wow", 44);
                System.out.println(instance);
                System.out.println("Did it: " + instance.doIt());
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // This is a bit hackish, as we're going through the AbstractFileClassLoader mapping from AbstractFile
    // (a virtualized file) to the compiled class byte code (which isn't directly accessible, as far as I can tell).
    private static Collection<Class<? extends MustConform>> compiledConformers(IMain interpreter) {
        Collection<Class<? extends MustConform>> retval = new LinkedList<>();
        AbstractFileClassLoader interpreterClassLoader = interpreter.classLoader();

        AbstractFile root = interpreterClassLoader.root();
        Iterator<AbstractFile> it = root.iterator();
        while(it.hasNext()) {
            AbstractFile classFile = it.next();
            String name = classFile.name().replace(".class", "");
            Class<?> clazz = interpreterClassLoader.findClass(name);
            if(MustConform.class.isAssignableFrom(clazz)) {
                System.out.println("Found conforming class: " + name);
                retval.add((Class<? extends MustConform>) clazz);
            }
        }

        return retval;
    }
}
