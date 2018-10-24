/*
Daniel Bastarrica y Gabriel Sellés declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.
*/

//EJERCICIO 2 - HADOOP

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountInFile {

  public static class Pair implements Writable{
  	
    public Text fileID;
  	public IntWritable num;

    public Pair(){
      this.fileID = new Text();
      this.num =  new IntWritable();
    }

    public Pair(String f, int n){
      this.fileID = new Text(f);
      this.num = new IntWritable(n);
    }

    public Pair(Text f, IntWritable n){
      this.fileID = f;
      this.num = n;
    }

    public void set(String f, int n) {
      this.fileID = new Text(f);
      this.num = new IntWritable(n);
    }

    public void set(Text f, IntWritable n){
      this.fileID = f;
      this.num = n;
    }

    public void write (DataOutput out) throws IOException {
      fileID.write(out);
      num.write(out);
    }

    public void readFields (DataInput in) throws IOException {
      this.fileID.readFields(in);
      this.num.readFields(in);
    }

    @Override
    public String toString(){
      return " <" + fileID.toString() + ", " + num.get() + ">";
    }

  }

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop, el mapper recibe cada linea del archivo de entrada como
   * argumento. La función "map" parte cada línea y para cada palabra emite la
   * pareja (word,1) como salida.</p>
   */
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Pair>{ 

    private static String patronPalabra = "[a-zA-Z]*";
    private static String patronSignos = "^[\\p{Punct}¡¿]*|[\\p{Punct}¡¿]*$";

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String inputFile = ((FileSplit) context.getInputSplit()).getPath().getName();

      StringTokenizer it = new StringTokenizer(value.toString());

      while (it.hasMoreTokens()) {
        String word_aux = it.nextToken().toLowerCase();

        //Quitamos los caracteres al principio y final de las palabras

        word_aux = word_aux.replaceAll(patronSignos, "");

        if( word_aux.length() > 0 && word_aux.matches(patronPalabra) ) //es palabra válida
          context.write( new Text(word_aux), new Pair( inputFile, 1) );

      }

    }
  }

  public static class MyCombiner extends Reducer<Text, Pair, Text, Pair>{

        public void reduce(final Text key, final Iterable<Pair> values, 
        		final Context context) throws IOException, InterruptedException{

        	HashMap <String, Integer> map = new HashMap <String, Integer> ();
        	for (Pair pair : values) {
        	  if(map.containsKey(pair.fileID.toString()))
        	    map.put(pair.fileID.toString(), map.get(pair.fileID.toString()) + pair.num.get() );
        	  else
        	    map.put(pair.fileID.toString(), pair.num.get() );
        	  
        	}

        	for (String file : map.keySet()){
        		context.write(key, new Pair( file, map.get(file) ) );
        	}

        }

    }
  
  /**
   * <p>La función "reduce" recibe los valores (apariciones) asociados a la misma
   * clave (palabra) como entrada y produce una pareja con la palabra y el número
   * total de apariciones. 
   * Ojo: como las parejas generadas por la función "map" siempre tienen como 
   * valor 1 se podría evitar la suma y devolver directamente el número de 
   * elementos.</p>  
   */
  public static class MyReducer 
       extends Reducer<Text,Pair,Text,Pair> { /*******************************************/

    public void reduce(Text key, Iterable<Pair> values, Context context) 
	                              throws IOException, InterruptedException {

	    HashMap <String, Integer> map = new HashMap <String, Integer> ();
	    String file = "";
	    int mejor = 0;
	    for (Pair pair : values) {
		    if(map.containsKey(pair.fileID.toString())){
		        int total = map.get(pair.fileID.toString()) + pair.num.get() ;
		        map.put(pair.fileID.toString(), total);
		        if (total > mejor) {
			        mejor = total;
		    	    file = pair.fileID.toString();
		        }
		    }
	        else{
		        map.put(pair.fileID.toString(), pair.num.get() );
		       	if (pair.num.get() > mejor) {
			        mejor = pair.num.get();
			        file = pair.fileID.toString();
		        }
	        }
	      }
	      
	      context.write(key, new Pair( file, mejor) );
	    }
	  }

  /**
   * <p>Clase principal con método main que iniciará la ejecución de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(WordCountInFile.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Pair.class);
    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Pair.class); 

    // Archivos de entrada y directorio de salida
    
    String raiz = "ficheros/02_words/sci.space";
    //int total = 0;
    for( final File archivo : new File(raiz).listFiles() )
    	if(!archivo.isDirectory())
    		FileInputFormat.addInputPath(job, new Path( raiz + '/' + archivo.getName() ));
    		//System.out.println(raiz + '/' + archivo.getName());
    		//total++ ;
    //System.out.println("Total de archivos: " + total);

	//FileInputFormat.addInputPath(job, new Path( raiz + '/' + "59497" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida"));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Cada reducer genera un fichero  'part-r-XXXXX'
    job.setNumReduceTasks(1);

	// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(false) ? 0 : 1);
  }
}

