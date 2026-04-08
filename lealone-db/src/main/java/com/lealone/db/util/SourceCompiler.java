/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.Utils;
import com.lealone.db.service.Service;

/**
 * This class allows to convert source code to a class. It uses one class loader per class.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SourceCompiler {

    private final static HashMap<String, LinkedList<JavaFileObject>> jfos = new HashMap<>();
    private final static HashMap<String, SCJavaFileObject> classes = new HashMap<>();

    /**
     * The class name to source code map.
     */
    private final HashMap<String, String> sources = new HashMap<>();

    /**
     * The class name to byte code map.
     */
    private final HashMap<String, Class<?>> compiled = new HashMap<>();

    private File classDir;

    public File getClassDir() {
        return classDir;
    }

    public void setClassDir(File classDir) {
        this.classDir = classDir;
    }

    /**
     * Set the source code for the specified class.
     * This will reset all compiled classes.
     *
     * @param className the class name
     * @param source the source code
     */
    public void setSource(String className, String source) {
        String packageName;
        int idx = className.lastIndexOf('.');
        if (idx >= 0) {
            packageName = className.substring(0, idx);
        } else {
            packageName = "";
        }
        SCJavaFileObject jfo = new SCJavaFileObject(className, source, classDir);
        LinkedList<JavaFileObject> list = jfos.get(packageName);
        if (list == null) {
            list = new LinkedList<>();
            jfos.put(packageName, list);
        }
        list.add(jfo);
        sources.put(className, source);
        compiled.clear();
    }

    /**
     * Get the first public static method of the given class.
     *
     * @param className the class name
     * @return the method name
     */
    public Method getMethod(String className) throws ClassNotFoundException {
        Class<?> clazz = getClass(className);
        Method[] methods = clazz.getDeclaredMethods();
        for (Method m : methods) {
            int modifiers = m.getModifiers();
            if (Modifier.isPublic(modifiers)) {
                if (Modifier.isStatic(modifiers)) {
                    return m;
                }
            }
        }
        return null;
    }

    public Class<?> getClass(String fullName) throws ClassNotFoundException {
        Class<?> compiledClass = compiled.get(fullName);
        if (compiledClass != null) {
            return compiledClass;
        }
        URL[] urls;
        if (classDir != null) {
            try {
                urls = new URL[] { classDir.toURI().toURL() };
            } catch (MalformedURLException e) {
                throw DbException.convert(e);
            }
        } else {
            urls = new URL[0];
        }
        ClassLoader classLoader = new URLClassLoader(urls, getClass().getClassLoader()) {
            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                Class<?> classInstance = compiled.get(name);
                if (classInstance == null) {
                    String source = sources.get(name);
                    if (source == null) {
                        classInstance = super.findClass(name);
                        compiled.put(name, classInstance);
                        return classInstance;
                    }
                    String packageName = null;
                    int idx = name.lastIndexOf('.');
                    String className;
                    if (idx >= 0) {
                        packageName = name.substring(0, idx);
                        className = name.substring(idx + 1);
                    } else {
                        className = name;
                    }
                    byte[] data = compile(this, packageName, className, name, source, classDir);
                    if (data == null) {
                        classInstance = findSystemClass(name);
                    } else {
                        classInstance = defineClass(name, data, 0, data.length);
                        compiled.put(name, classInstance);
                    }
                }
                return classInstance;
            }
        };
        return classLoader.loadClass(fullName);
    }

    public Class<?> compile(String fullName) {
        try {
            return getClass(fullName);
        } catch (ClassNotFoundException e) {
            throw DbException.convert(e);
        }
    }

    private static byte[] compile(ClassLoader classLoader, String packageName, String className,
            String fullName, String source, File classDir) {
        if (!source.startsWith("package ")) {
            StringBuilder buff = new StringBuilder();
            int endImport = source.indexOf("@CODE");
            String importCode = "import java.util.*;\n" + "import java.math.*;\n"
                    + "import java.sql.*;\n";
            if (endImport >= 0) {
                importCode = source.substring(0, endImport);
                source = source.substring("@CODE".length() + endImport);
            }
            if (packageName != null) {
                buff.append("package " + packageName + ";\n");
            }
            buff.append(importCode).append("\n");
            buff.append(
                    "public class " + className + " {\n" + "    public static " + source + "\n" + "}\n");
            source = buff.toString();
        }
        return compile(classLoader, fullName, source, classDir);
    }

    private static byte[] compile(ClassLoader classLoader, String fullName, String sourceCode,
            File classDir) {
        try {
            SCClassLoader cl;
            if (classLoader instanceof SCClassLoader)
                cl = (SCClassLoader) classLoader;
            else
                cl = new SCClassLoader(classLoader);
            return SCJavaCompiler.newInstance(cl, true).compile(fullName, sourceCode, classDir);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static <T> T compileAsInstance(String className, String sourceCode) {
        Class<?> clz = compileAsClass(className, sourceCode);
        return Utils.newInstance(clz);
    }

    public static Class<?> compileAsClass(String className, String sourceCode) {
        // 不能直接传递参数classLoader给compile，要传自定义SCClassLoader，否则会有各种类找不到的问题
        SCClassLoader cl = new SCClassLoader(SourceCompiler.class.getClassLoader());
        byte[] bytes = compile(cl, className, sourceCode, null);
        return cl.getClass(className, bytes);
    }

    private static class SCClassLoader extends ClassLoader {

        public SCClassLoader(ClassLoader parent) {
            super(parent);
        }

        public Class<?> getClass(String className, byte[] bytes) {
            return defineClass(className, bytes, 0, bytes.length);
        }
    }

    private static class SCJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

        private final SCClassLoader classLoader;

        protected SCJavaFileManager(JavaFileManager fileManager, SCClassLoader classLoader) {
            super(fileManager);
            this.classLoader = classLoader;
        }

        @Override
        public ClassLoader getClassLoader(Location location) {
            return classLoader;
        }

        @Override
        public String inferBinaryName(Location location, JavaFileObject file) {
            if (file instanceof SCJavaFileObject) {
                SCJavaFileObject scf = (SCJavaFileObject) file;
                return scf.className;
            }
            return super.inferBinaryName(location, file);
        }

        @Override
        public Iterable<JavaFileObject> list(Location location, String packageName, Set<Kind> kinds,
                boolean recurse) throws IOException {
            Iterable<JavaFileObject> iterable = jfos.get(packageName);
            if (iterable != null)
                return iterable;
            else
                return super.list(location, packageName, kinds, recurse);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind,
                FileObject sibling) throws IOException {
            if (sibling != null && sibling instanceof SCJavaFileObject) {
                return ((SCJavaFileObject) sibling).addOutputJavaFile(className);
            }
            throw new IOException(
                    "The source file passed to getJavaFileForOutput() is not a SCJavaFileObject: "
                            + sibling);
        }
    }

    private static class SCJavaFileObject extends SimpleJavaFileObject {

        private final String className;
        private final String sourceCode;
        private final ByteArrayOutputStream outputStream;
        private Map<String, SCJavaFileObject> outputFiles;
        private File classDir;

        public SCJavaFileObject(String className, String sourceCode, File classDir) {
            super(makeURI(className), Kind.SOURCE);
            this.className = className;
            this.sourceCode = sourceCode;
            this.outputStream = null;
            this.classDir = classDir;
        }

        private SCJavaFileObject(String packageName, File f, Kind kind) {
            super(f.toURI(), kind);
            this.className = packageName + "." + f.getName().substring(0, f.getName().length() - 6);
            this.sourceCode = null;
            this.outputStream = null;
        }

        private SCJavaFileObject(String name, Kind kind, File classDir) {
            super(makeURI(name), kind);
            this.className = name;
            this.outputStream = new ByteArrayOutputStream();
            this.sourceCode = null;
            this.classDir = classDir;
        }

        public boolean isCompiled() {
            return (outputFiles != null);
        }

        // public Map<String, byte[]> getClassByteCodes() {
        // Map<String, byte[]> results = new HashMap<>();
        // for (SCJavaFileObject outputFile : outputFiles.values()) {
        // results.put(outputFile.className, outputFile.outputStream.toByteArray());
        // }
        // return results;
        // }

        public byte[] getClassByteCode(String className) {
            byte[] bytes = outputFiles.get(className).outputStream.toByteArray();
            if (classDir != null) {
                for (SCJavaFileObject outputFile : outputFiles.values()) {
                    className = outputFile.className;
                    String packageName = "";
                    int idx = className.lastIndexOf('.');
                    if (idx >= 0) {
                        packageName = className.substring(0, idx);
                        className = className.substring(idx + 1);
                    }
                    Service.writeClassFile(classDir.getAbsolutePath(), packageName, className,
                            outputFile.outputStream.toByteArray());
                }
            }
            return bytes;
        }

        public SCJavaFileObject addOutputJavaFile(String className) {
            if (outputFiles == null) {
                outputFiles = new LinkedHashMap<>();
            }
            SCJavaFileObject outputFile = new SCJavaFileObject(className, Kind.CLASS, classDir);
            outputFiles.put(className, outputFile);
            classes.put(className, outputFile);
            return outputFile;
        }

        @Override
        public Reader openReader(boolean ignoreEncodingErrors) throws IOException {
            return new StringReader(sourceCode);
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return sourceCode;
        }

        @Override
        public OutputStream openOutputStream() {
            return outputStream;
        }

        @Override
        public InputStream openInputStream() throws IOException {
            return toUri().toURL().openStream();
        }

        private static URI makeURI(final String canonicalClassName) {
            int dotPos = canonicalClassName.lastIndexOf('.');
            String simpleClassName = dotPos == -1 ? canonicalClassName
                    : canonicalClassName.substring(dotPos + 1);
            try {
                return new URI(simpleClassName + Kind.SOURCE.extension);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SCJavaCompiler {

        private final boolean debug;
        private final JavaCompiler compiler;
        private final Collection<String> compilerOptions;
        private final DiagnosticListener<JavaFileObject> listener;
        private final SCJavaFileManager fileManager;

        public static SCJavaCompiler newInstance(SCClassLoader classLoader, boolean debug) {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) {
                throw new RuntimeException("JDK Java compiler not available");
            }
            return new SCJavaCompiler(compiler, classLoader, debug);
        }

        private SCJavaCompiler(JavaCompiler compiler, SCClassLoader classLoader, boolean debug) {
            this.debug = debug;
            this.compiler = compiler;
            this.listener = new SCDiagnosticListener();
            this.fileManager = new SCJavaFileManager(
                    compiler.getStandardFileManager(listener, null, Charset.forName("UTF-8")),
                    classLoader);
            ArrayList<String> list = new ArrayList<>();
            list.add(this.debug ? "-g:source,lines,vars" : "-g:none");
            this.compilerOptions = list;
        }

        public byte[] compile(String fullName, String sourceCode, File classDir)
                throws IOException, ClassNotFoundException {
            return doCompile(fullName, sourceCode, classDir).getClassByteCode(fullName);
        }

        private SCJavaFileObject doCompile(String fullName, String sourceCode, File classDir)
                throws IOException, ClassNotFoundException {
            SCJavaFileObject compilationUnit = new SCJavaFileObject(fullName, sourceCode, classDir);
            CompilationTask task = compiler.getTask(null, fileManager, listener, compilerOptions, null,
                    Collections.singleton(compilationUnit));
            if (!task.call()) {
                throw new RuntimeException("Compilation failed", null);
            } else if (!compilationUnit.isCompiled()) {
                throw new ClassNotFoundException(fullName + ": Class file not created by compilation.");
            }
            return compilationUnit;
        }
    }

    private static class SCDiagnosticListener implements DiagnosticListener<JavaFileObject> {

        private static final Logger logger = LoggerFactory.getLogger(SCDiagnosticListener.class);

        @Override
        public void report(Diagnostic<? extends JavaFileObject> diagnostic) {
            if (diagnostic.getKind() == javax.tools.Diagnostic.Kind.ERROR) {
                String message = diagnostic.toString() + " (" + diagnostic.getCode() + ")";
                logger.error(message);
            } else if (logger.isTraceEnabled()) {
                logger.trace(diagnostic.toString() + " (" + diagnostic.getCode() + ")");
            }
        }
    }
}
