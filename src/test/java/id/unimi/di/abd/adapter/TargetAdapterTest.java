package id.unimi.di.abd.adapter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
public class TargetAdapterTest {

    @TempDir File dir;
    public static final String EXT = ".tmp";

    @Test
    public void appendExtTest(){
        String name = "foo";
        String output = TargetAdapter.appendExt(name,EXT);
        assertThat(output).isEqualTo("foo.tmp");
    }

    @Test
    public void verifyThatFileIsRenamed() throws NoSuchFieldException, IllegalAccessException, IOException {
        String fileName = "name";
        TargetAdapter targetAdapter = getTargetAdapter(fileName);
        targetAdapter.finish();
        verify(targetAdapter, times(1)).renameFile(fileName+EXT, fileName);
    }


    private TargetAdapter getTargetAdapter(String fileName) throws NoSuchFieldException, IllegalAccessException {
        TargetAdapter targetAdapter = Mockito.mock(
                TargetAdapter.class,
                CALLS_REAL_METHODS
        );
        Field fileField = targetAdapter.getClass().getSuperclass().getDeclaredField("file");
        fileField.setAccessible(true);
        fileField.set(targetAdapter, new File(dir, TargetAdapter.appendExt(fileName,EXT)));
        Field extField = targetAdapter.getClass().getSuperclass().getDeclaredField("ext");
        extField.setAccessible(true);
        extField.set(targetAdapter, EXT);
        return targetAdapter;
    }
}
