package at.ac.is161505.hastag;

import at.ac.is161505.hashtag.HashtagCount1c;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This file is part of hashtag.
 * <p>
 * hashtag is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * hashtag is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * Diese Datei ist Teil von hashtag.
 * <p>
 * hashtag ist Freie Software: Sie können es unter den Bedingungen
 * der GNU General Public License, wie von der Free Software Foundation,
 * Version 3 der Lizenz oder (nach Ihrer Wahl) jeder späteren
 * veröffentlichten Version, weiterverbreiten und/oder modifizieren.
 * <p>
 * hashtag wird in der Hoffnung, dass es nützlich sein wird, aber
 * OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
 * Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
 * Siehe die GNU General Public License für weitere Details.
 * <p>
 * Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
 * Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.
 * <p>
 * Created by n17405180 on 21.10.17.
 */
public class JoinResultsReduceTests {

    @Test
    public void shouldJoinResults() throws IOException, InterruptedException {

        HashtagCount1c.JoinResultsReduce a = new HashtagCount1c.JoinResultsReduce();

        List<Text> users = new ArrayList<Text>();
        users.add(new Text("user1"));

        List<Text> counts = new ArrayList<Text>();
        counts.add(new Text("1234"));

        a.reduce(new Text("USER|#tag1"), users, null);
        a.reduce(new Text("USER|#tag2"), users, null);
        a.reduce(new Text("USER|#tag3"), users, null);
        a.reduce(new Text("USER|#tag4"), users, null);
        a.reduce(new Text("USER|#tag5"), users, null);

        a.reduce(new Text("TOPN|#tag1"), counts, null);
        a.reduce(new Text("TOPN|#tag2"), counts, null);
        a.reduce(new Text("TOPN|#tag3"), counts, null);
        a.reduce(new Text("TOPN|#tag4"), counts, null);
        a.reduce(new Text("TOPN|#tag5"), counts, null);


        assertTrue(a.userMap.size() == 5);
        assertTrue(a.topList.size() == 5);

    }
}
