package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static demo.KStreamWordCounter.INPUT_TOPIC;
import static java.lang.String.valueOf;
import static java.util.concurrent.ThreadLocalRandom.current;


public class KStreamWordCounterProducer {
    public static final List<String> ARTICLE = new ArrayList<>();
    static {
        ARTICLE.add("A 48 hour effort by the Trump administration to soothe jittery financial markets did little to reverse the free fall in stocks on Monday as the president’s renewed attack on the Federal Reserve and the specter of a prolonged government shutdown further rattled investors already worried about a global economic slowdown");
        ARTICLE.add("With a single tweet on Monday President Trump undercut his top economic advisers efforts to reassure the markets that he did not intend to fire Jerome H Powell as Fed chairman But Mr Trump who blames the Fed’s recent interest rate increases for the market gyrations said the only problem our economy has is the Fed an assertion that exacerbated the worst sell-off on Wall Street since the 2008 financial crisis");
        ARTICLE.add("Stocks continued marching toward their largest December declines since the 1930s with the S&P 500 closing down 27 percent on Monday after a shortened trading session ahead of the Christmas holiday");
        ARTICLE.add("The markets are trying to digest the confusing signals from Washington On Saturday the Treasury secretary Steven Mnuchin posted what he said was a quote from Mr Trump acknowledging that while he did not agree with the Fed’s recent interest rate increases he did not have any plans to fire Mr Powell And Mick Mulvaney Mr Trump’s incoming chief of staff said on ABC’s This Week that Mr Trump now realized he lacked authority to fire Mr Powell his handpicked chairman");
        ARTICLE.add("Advertisement");
        ARTICLE.add("For the markets the Fed is a complicated player at the moment Investors have been disappointed by rising interest rates concerned that they could sap growth But the speculation about firing Mr Powell a move that could turn the independent central bank into a political tool has undermined confidence in a pivotal institution essential to economic policy");
        ARTICLE.add("Markets have already been on edge in recent weeks because of uncertainty surrounding trade negotiations between the United States and China signs of slowing global growth and the prospect of a prolonged government shutdown in the United States Mr Mnuchin added to the economic jitters by announcing on Sunday that he had called bank executives to ensure the markets were functioning properly the type of discussions usually reserved for moments of crisis");
        ARTICLE.add("You have 4 free articles remaining");
        ARTICLE.add("Subscribe to The Times");
        ARTICLE.add("It signaled a sense of panic and anxiety that didn’t need to be there said Brian Gardner an analyst at the investment banking firm Keefe Bruyette & Woods My first reaction when I heard it was what has happened over the last couple of days that the market does not understand or realize? Is there something that Treasury knows that the rest of us don’t");
        ARTICLE.add("And while Mr Mnuchin and Mr Mulvaney tried to reassure markets on the Fed’s leadership there is still an effort within the White House to discern whether any legal rationale exists for removing Mr Powell from the chairman spot according to two people familiar with the discussions");
        ARTICLE.add("Peter Navarro Mr Trump’s top trade adviser has been openly critical of the Fed’s decision to raise interest rates and blamed the central bank for the market volatility Mr Mnuchin meanwhile has said the recent stock swings are the result of high-speed electronic trading and post-crisis financial rules that he says make it harder for banks to play their traditional market-stabilization role");
        ARTICLE.add("Editors’ Picks");
        ARTICLE.add("2018: The Year in Stuff");
        ARTICLE.add("How McKinsey Has Helped Raise the Stature of Authoritarian Governments");
        ARTICLE.add("Suicide Quarterbacks and the Hilinski Family");
        ARTICLE.add("Advertisement");
        ARTICLE.add("Mr Trump on Monday made clear he is still not happy with Mr Powell likening the Fed to a powerful golfer who can’t score because he has no touch - he can’t putt");
        ARTICLE.add("Those attacks have drawn criticism from former Fed officials as well as lawmakers from both parties who say that any attempt to turn the Fed into a political organ would undermine its credibility Most developed nations have granted their central banks considerable autonomy over policymaking precisely because of concerns that politicians would seek to increase short-term growth at the expense of inflation and instability");
        ARTICLE.add("On Monday Senator Jeff Flake the retiring Arizona Republican posted a photograph on Twitter of a 5-billion-dollar bank note from Zimbabwe and urged the president to back off");
        ARTICLE.add("I’ve lived in countries where the Head of State has used the central bank for political purposes he said Please respect the Fed’s independence Mr President");
    }

    public static void main(String... args) throws Exception {
        System.out.println("Producer topic=" + INPUT_TOPIC);
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka1:9092");
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("retries", 10);
        props.put("enable.idempotence", true);
        Producer<String, String> producer = new KafkaProducer(props);

        for (String line : ARTICLE)  {
            String key = valueOf(current().nextInt(16));
            System.out.printf("Send message: %s:%s\n", key, line);
            producer.send(new ProducerRecord(INPUT_TOPIC, key, line));
            Thread.sleep(5);
        }
    }
}
