import java.io.PrintStream;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext.;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.MatchError;
import scala.Predef.;
import scala.ScalaObject;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq.;
import scala.collection.SeqLike;
import scala.collection.TraversableLike;
import scala.collection.immutable.Range.Inclusive;
import scala.collection.immutable.StringLike;
import scala.collection.mutable.StringBuilder;
import scala.reflect.ClassManifest.;
import scala.reflect.Manifest.;
import scala.reflect.OptManifest;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction1.mcDD.sp;
import scala.runtime.AbstractFunction1.mcVI.sp;
import scala.runtime.AbstractFunction2.mcDDD.sp;
import scala.runtime.BoxesRunTime;
import scala.runtime.ObjectRef;
import scala.runtime.RichInt;

public final class PageRank$
  implements ScalaObject
{
  public static final  MODULE$;
  
  private PageRank$()
  {
    MODULE$ = this;
  }
  
  public void main(String[] args)
  {
    if (args.length < 4)
    {
      System.err.println("Usage: PageRank <master> <file> <number_of_iterations> <save_path> [<slices>]");
      System.exit(1);
    }
    int iters = Predef..MODULE$.augmentString(args[2]).toInt();
    int slices = 1;
    String save_path = args[3];
    if (args.length > 4) {
      slices = Predef..MODULE$.augmentString(args[4]).toInt();
    }
    null;null;SparkContext ctx = new SparkContext(args[0], "PageRank", BigDataBenchConf..MODULE$.SPARK_HOME(), (Seq)Seq..MODULE$.apply(Predef..MODULE$.wrapRefArray((Object[])new String[] { BigDataBenchConf..MODULE$.TARGET_JAR_BIGDATABENCH() })), null, null);
    
    RDD lines = ctx.textFile(args[1], slices);
    
    RDD links$1 = 
    
      SparkContext..MODULE$.rddToPairRDDFunctions(lines.map(new AbstractFunction1()
      {
        public static final long serialVersionUID = 0L;
        
        public final Tuple2<String, String> apply(String s)
        {
          String[] parts = s.split("\\s+");
          return new Tuple2(parts[0], parts[1]);
        }
      }, ClassManifest..MODULE$.classType(Tuple2.class, ClassManifest..MODULE$.classType(String.class), Predef..MODULE$.wrapRefArray((Object[])new OptManifest[] { ClassManifest..MODULE$.classType(String.class) })))
      .distinct(), ClassManifest..MODULE$.classType(String.class), ClassManifest..MODULE$.classType(String.class)).groupByKey().cache();
    
    Predef..MODULE$.println(new StringBuilder().append(BoxesRunTime.boxToLong(links$1.count()).toString()).append(" links loaded.").toString());
    
    final ObjectRef ranks$1 = new ObjectRef(SparkContext..MODULE$.rddToPairRDDFunctions(links$1, ClassManifest..MODULE$.classType(String.class), ClassManifest..MODULE$.classType(Seq.class, ClassManifest..MODULE$.classType(String.class), Predef..MODULE$.wrapRefArray((Object[])new OptManifest[0]))).mapValues(new AbstractFunction1()
    {
      public static final long serialVersionUID = 0L;
      
      public final double apply(Seq<String> v)
      {
        return 1.0D;
      }
    }));
    Predef..MODULE$.intWrapper(1).to(iters).foreach$mVc$sp(new AbstractFunction1.mcVI.sp()
    {
      public static final long serialVersionUID = 0L;
      private final RDD links$1;
      
      public final void apply(int i)
      {
        apply$mcVI$sp(i);
      }
      
      public void apply$mcVI$sp(int v1)
      {
        RDD contribs = SparkContext..MODULE$.rddToPairRDDFunctions(SparkContext..MODULE$.rddToPairRDDFunctions(this.links$1, ClassManifest..MODULE$.classType(String.class), ClassManifest..MODULE$.classType(Seq.class, ClassManifest..MODULE$.classType(String.class), Predef..MODULE$.wrapRefArray((Object[])new OptManifest[0]))).join((RDD)ranks$1.elem), ClassManifest..MODULE$.classType(String.class), ClassManifest..MODULE$.classType(Tuple2.class, ClassManifest..MODULE$.classType(Seq.class, ClassManifest..MODULE$.classType(String.class), Predef..MODULE$.wrapRefArray((Object[])new OptManifest[0])), Predef..MODULE$.wrapRefArray((Object[])new OptManifest[] { Manifest..MODULE$.Double() }))).values().flatMap(new AbstractFunction1()
        {
          public static final long serialVersionUID = 0L;
          
          public final Seq<Tuple2<String, Object>> apply(Tuple2<Seq<String>, Object> ???)
          {
            Object localObject = ???;
            if (localObject != null)
            {
              Seq localSeq1 = (Seq)((Tuple2)localObject)._1();double d1 = BoxesRunTime.unboxToDouble(((Tuple2)localObject)._2());
              Seq urls = localSeq1;final double rank$1 = d1;
              int size$1 = urls.size();
              (Seq)urls.map(new AbstractFunction1()
              {
                public static final long serialVersionUID = 0L;
                
                public final Tuple2<String, Object> apply(String url)
                {
                  return new Tuple2(url, BoxesRunTime.boxToDouble(rank$1 / this.size$1));
                }
              }, Seq..MODULE$.canBuildFrom());
            }
            throw new MatchError(localObject);
          }
        }, ClassManifest..MODULE$.classType(Tuple2.class, ClassManifest..MODULE$.classType(String.class), Predef..MODULE$.wrapRefArray((Object[])new OptManifest[] { Manifest..MODULE$.Double() })));
        
        ranks$1.elem = SparkContext..MODULE$.rddToPairRDDFunctions(SparkContext..MODULE$.rddToPairRDDFunctions(contribs, ClassManifest..MODULE$.classType(String.class), Manifest..MODULE$.Double()).reduceByKey(new AbstractFunction2.mcDDD.sp()
        {
          public static final long serialVersionUID = 0L;
          
          public double apply$mcDDD$sp(double v1, double v2)
          {
            return v1 + v2;
          }
          
          public final double apply(double paramAnonymous2Double1, double paramAnonymous2Double2)
          {
            return apply$mcDDD$sp(paramAnonymous2Double1, paramAnonymous2Double2);
          }
        }), ClassManifest..MODULE$.classType(String.class), Manifest..MODULE$.Double()).mapValues(new AbstractFunction1.mcDD.sp()
        {
          public static final long serialVersionUID = 0L;
          
          public double apply$mcDD$sp(double v1)
          {
            return 0.15D + 0.85D * v1;
          }
          
          public final double apply(double paramAnonymous2Double)
          {
            return apply$mcDD$sp(paramAnonymous2Double);
          }
        });
      }
    });
    Predef..MODULE$.println(new StringBuilder().append("Result saved to: ").append(save_path).toString());
    ((RDD)ranks$1.elem).saveAsTextFile(save_path);
    
    System.exit(0);
  }
  
  static
  {
    new ();
  }
}

