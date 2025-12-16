package from_csv;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;


public class chinesetopinyin {

    private static boolean isChinese(char c) {
        return (c >= '\u4e00' && c <= '\u9fa5');
    }

    private static boolean isEnglish(char c) {
        return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
    }

    public static String addUnderscoreBetweenChineseAndEnglish(String str) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            result.append(c);
            if (i < str.length() - 1) {
                char nextChar = str.charAt(i + 1);
                if ((isChinese(c) && isEnglish(nextChar)) || (isEnglish(c) && isChinese(nextChar))) {
                    result.append("_");
                }
            }
        }
        return result.toString();
    }



    public static String chineseToPinyin(String chinese) {
        //下一行代码是给中文英文之间加上下划线
        chinese = addUnderscoreBetweenChineseAndEnglish(chinese);

        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        format.setVCharType(HanyuPinyinVCharType.WITH_V);
        StringBuilder pinyin = new StringBuilder();
        for (char c : chinese.toCharArray()) {
            try {
                String[] pinyinArray = PinyinHelper.toHanyuPinyinStringArray(c, format);
                if (pinyinArray != null && pinyinArray.length > 0) {
                    pinyin.append(pinyinArray[0]);
                } else {
                    pinyin.append(c);
                }
            } catch (BadHanyuPinyinOutputFormatCombination e) {
                pinyin.append(c);
            }
        }
        String result = pinyin.toString();
        result = convertSpecialCharacters(result);
        //result = removeLeadingAndTrailingUnderscores(result).toLowerCase();
        result = removeLeadingAndTrailingUnderscores(result);
        return result;
    }

    //将特殊符号转化为下划线
    private static String convertSpecialCharacters(String input) {
        return input.replaceAll("[^a-zA-Z0-9\\u4E00-\\u9FA5]+", "_");
    }

    //将字段左右两侧的下划线清除
    private static String removeLeadingAndTrailingUnderscores(String input) {
        return input.replaceAll("^_*|_*$", "");
    }


    public static void main(String[] args) {
        String chinese = "aBfa'（汉字zhongguo） fu出品";
        String pinyin = chineseToPinyin(chinese);
        System.out.println(pinyin);
    }
}
