# Auto-Advance to Next Game Date Feature

## Overview

The frontend now automatically advances to the next available game date when a user loads or selects a date with no scheduled games.

## How It Works

### Configuration
```javascript
const AUTO_ADVANCE_TO_NEXT_GAME = true; // Set to false to disable
```

### Behavior

**When Enabled (default):**
- If the selected date has no games, the UI automatically advances to the next future date with scheduled games
- The date picker is updated to show the advanced date
- Works on initial page load and when users manually select dates

**When Disabled:**
- Shows "No games on {date}" message
- User must manually navigate to a date with games

### Examples

#### Scenario 1: Loading on an Off-Day
- User visits the site on **October 18, 2025** (no games scheduled)
- System automatically advances to **October 21, 2025** (Opening Night)
- Date picker shows "2025-10-21"
- Games are displayed immediately

#### Scenario 2: Selecting an Off-Day
- User manually selects **October 19, 2025** from date picker
- User clicks "Apply Date"
- System detects no games on that date
- Automatically advances to **October 21, 2025**
- Date picker updates to show "2025-10-21"

#### Scenario 3: Historical Viewing
- User selects a past date with games (e.g., preseason game on Oct 10)
- Games are displayed normally
- **No auto-advance occurs** (date has games)

### Implementation Details

**Helper Function:**
```javascript
const nextGameDate = (fromDate) => {
  // Finds the first date AFTER fromDate that has games
  // Returns null if no future games exist
}
```

**Logic Flow:**
1. Check if `AUTO_ADVANCE_TO_NEXT_GAME` is enabled
2. Check if current date has games: `state.byDate.get(date)`
3. If no games, call `nextGameDate(date)` to find next available date
4. Update both the working date `d` and the picker value
5. Load predictions/odds for the new date
6. Render the page

### Key Features

✅ **Forward-looking only** - Never advances backward in time  
✅ **Preserves historical viewing** - Dates with games are never skipped  
✅ **Updates UI** - Date picker shows the advanced date  
✅ **Seamless UX** - No "No games" message on off-days  
✅ **Query param support** - `?date=2025-10-19` still respects auto-advance  
✅ **Configurable** - Can be disabled by setting `AUTO_ADVANCE_TO_NEXT_GAME = false`

### Complementary Features

This feature works alongside:
- `STRICT_SCHEDULE_DATES` - Forces exact schedule matching (overrides auto-advance)
- `PIN_DATE` - Forces a specific date (bypasses auto-advance)
- Query params - `?date=YYYY-MM-DD` still gets auto-advanced if no games

### Use Cases

**Perfect for:**
- Off-season viewing (no games for weeks)
- Mid-week off-days during regular season
- All-Star break
- Between playoff series

**Not needed when:**
- Every day has games (playoff season)
- User wants to see "No games" message explicitly
- Custom date filtering logic is required

## Testing

To test the feature:

1. **Test auto-advance on load:**
   - Visit `http://localhost:5051` on an off-day
   - Should auto-advance to next game date

2. **Test manual selection:**
   - Select an off-day from the date picker
   - Click "Apply Date"
   - Should auto-advance to next game

3. **Test with query param:**
   - Visit `http://localhost:5051?date=2025-10-19`
   - Should auto-advance to next game (Oct 21)

4. **Test with games:**
   - Select a date with games (e.g., Oct 21)
   - Should NOT auto-advance
   - Games should display normally

## Future Enhancements

Potential improvements:
- Add "Previous Game" / "Next Game" navigation buttons
- Show indicator when auto-advanced (e.g., "Advanced to next game: Oct 21")
- Add option to show "No games" instead of auto-advance (toggle in UI)
- Support backward auto-advance for historical viewing

## Related Files

- `web/app.js` - Main implementation
- `web/cards.html` - Canonical game cards UI
- `data/processed/schedule_2025_26.json` - Schedule data source

## Configuration Summary

```javascript
// Top of web/app.js
const STRICT_SCHEDULE_DATES = false;    // Force exact schedule dates
const AUTO_ADVANCE_TO_NEXT_GAME = true; // Auto-advance to next game
const PIN_DATE = '';                    // Force specific date (overrides both)
```

**Priority:** `PIN_DATE` > `STRICT_SCHEDULE_DATES` > `AUTO_ADVANCE_TO_NEXT_GAME`
