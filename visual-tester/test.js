const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const BASE_URL = 'http://localhost:8000';
const SCREENSHOT_DIR = path.join(__dirname, 'screenshots');
const PROFILE_DIR = path.join(__dirname, 'chrome-data');
const VIEWPORT = { width: 1440, height: 900 };

const PROMOTER_RA_ID = '83067';
const PROMOTER_NAME = 'Drop Dance Society';
const SOUND_ANCHORS = ['Kerri Chandler', 'Folamour', 'Dan Shake'];
const COMPETITOR_IDS = ['77801', '72357', '207515'];
const TEST_DATE = '2026-06-26';
const TEST_ARTIST = 'Chelina Manuhutu';

const wait = ms => new Promise(r => setTimeout(r, ms));
let issues = [];
let shotCount = 0;

function log(msg) { console.log('[' + new Date().toLocaleTimeString() + '] ' + msg); }
function issue(scr, sev, msg) { issues.push({screen:scr,severity:sev,msg}); console.log('  ' + (sev==='error'?'❌':'⚠️') + ' ' + msg); }

async function shot(page, name, desc, full) {
  shotCount++;
  var num = String(shotCount).padStart(2,'0');
  var fp = path.join(SCREENSHOT_DIR, num+'-'+name+'.png');
  await page.screenshot({path:fp, fullPage:!!full});
  log('📸 '+num+'-'+name+': '+desc);
}

// ═══ ONBOARDING ═══

async function onboardingStep1(page) {
  log('\n── ONBOARDING STEP 1: Profile ──');
  await wait(1000);
  await shot(page, 'onboard-1-init', 'Step 1 initial');

  await page.evaluate(() => {
    var btn = document.querySelector('#obTypePicker button[data-v="promoter"]');
    if (btn) btn.click();
  });
  await wait(500);
  log('  ✓ Selected Promoter');

  var city = await page.$eval('#obC', el => el.value).catch(()=>'');
  log('  City: ' + city);
  await wait(500);
  await shot(page, 'onboard-1-promoter', 'Promoter fields visible');

  var raInput = await page.$('#obPromoterRaId');
  if (raInput) {
    await raInput.click({clickCount:3});
    await raInput.type(PROMOTER_RA_ID, {delay:50});
    log('  Entered RA ID: ' + PROMOTER_RA_ID);
    await page.keyboard.press('Tab');
    await wait(5000);

    var raStatus = await page.$eval('#obPromoterRaStatus', el => el.textContent.trim()).catch(()=>'');
    log('  RA status: "' + raStatus + '"');
    if (raStatus.includes('DROP') || raStatus.includes('Drop')) log('  ✓ Promoter found');
    else issue('ob1','warn','RA lookup: "' + raStatus + '"');
  } else {
    issue('ob1','error','#obPromoterRaId not found');
  }

  var nameVal = await page.$eval('#obPromoterName', el => el.value).catch(()=>'');
  log('  Name auto-fill: "' + nameVal + '"');
  if (nameVal && !nameVal.toLowerCase().includes('drop') && nameVal.length > 0) {
    issue('ob1','error','BUG: Name shows "' + nameVal + '" instead of promoter name from RA');
  }

  var nameInput = await page.$('#obPromoterName');
  if (nameInput && !nameVal.toLowerCase().includes('drop')) {
    await nameInput.click({clickCount:3});
    await nameInput.type(PROMOTER_NAME, {delay:20});
  }

  await shot(page, 'onboard-1-filled', 'Step 1 filled');

  await page.evaluate(() => { if (typeof obDone === 'function') obDone(); });
  await wait(1500);
  log('  ✓ Next (obDone)');
}

async function onboardingStep2(page) {
  log('\n── ONBOARDING STEP 2: Sound ──');
  await wait(500);
  await shot(page, 'onboard-2-init', 'Step 2 initial');

  var ids = ['#obSoundArtist1','#obSoundArtist2','#obSoundArtist3'];
  for (var i = 0; i < SOUND_ANCHORS.length; i++) {
    var input = await page.$(ids[i]);
    if (input) {
      await input.click({clickCount:3});
      await input.type(SOUND_ANCHORS[i], {delay:30});
      await wait(800);
      var ac = await page.$('.ac-dropdown.open .ac-item');
      if (ac) { await ac.click(); log('  ✓ ' + SOUND_ANCHORS[i] + ' (autocomplete)'); }
      else { await page.keyboard.press('Tab'); log('  ✓ ' + SOUND_ANCHORS[i] + ' (typed)'); }
      await wait(400);
    }
  }

  await shot(page, 'onboard-2-filled', 'Step 2 with anchors');
  await page.evaluate(() => { if (typeof obStep2SoundDone === 'function') obStep2SoundDone(); });
  await wait(1500);
  log('  ✓ Next (obStep2SoundDone)');
}

async function onboardingStep3(page) {
  log('\n── ONBOARDING STEP 3: Competition ──');
  await wait(500);
  await shot(page, 'onboard-3-init', 'Step 3 initial');

  for (var ci = 0; ci < COMPETITOR_IDS.length; ci++) {
    var compId = COMPETITOR_IDS[ci];
    var raInput = await page.$('#obCompetitorRaId');
    if (raInput) {
      await raInput.click({clickCount:3});
      await raInput.type(compId, {delay:50});
      log('  Entering: ' + compId);
      await page.keyboard.press('Tab');
      await wait(4000);

      var compName = await page.$eval('#obCompetitorName', el => el.value).catch(()=>'');
      log('  Name: "' + compName + '"');

      var canAdd = await page.evaluate(() => {
        var btn = document.getElementById('obCompAddBtn');
        return btn && btn.style.pointerEvents !== 'none' && !btn.disabled;
      });

      if (canAdd) {
        await page.click('#obCompAddBtn');
        await wait(500);
        log('  ✓ Added ' + compId + ' (' + compName + ')');
      } else {
        issue('ob3','warn','Add disabled for ' + compId);
        await page.evaluate(() => { if (typeof _addCompetitor === 'function') _addCompetitor(); });
        await wait(500);
      }
    }
    if (ci === 0) await shot(page, 'onboard-3-first', 'First competitor added');
  }

  // Click a genre button
  await page.evaluate(() => {
    var btn = document.querySelector('#obGenrePicker button[data-genre="house"]');
    if (btn) btn.click();
  });
  await wait(200);
  log('  ✓ Selected House genre');

  var btnText = await page.$eval('#obStep3Btn', el => el.textContent.trim()).catch(()=>'');
  log('  Button: "' + btnText + '"');
  if (btnText.includes('Build') || btnText.includes('DNA')) issue('ob3','error','Button says "' + btnText + '"');

  await shot(page, 'onboard-3-ready', 'Step 3 ready');

  await page.evaluate(() => { if (typeof obStep3Done === 'function') obStep3Done(); });
  await wait(2000);
  log('  ✓ Started analysis');
}

async function onboardingStep4(page) {
  log('\n── ONBOARDING STEP 4: Analysis ──');
  await shot(page, 'onboard-4-start', 'Analysis starting');

  for (var i = 0; i < 180; i++) {
    await wait(1000);
    if (i > 0 && i % 20 === 0) {
      var phase = await page.$eval('#dnaPhaseLabel', el => el.textContent).catch(()=>'');
      log('  ' + i + 's: "' + phase + '"');
      if (phase.includes('DNA')) issue('ob4','error','"DNA" in label: "' + phase + '"');
    }
    if (i === 15 || i === 45) await shot(page, 'onboard-4-at-' + i + 's', 'Progress');

    // Check if summary CTA button appeared (shows after analysis)
    var summaryBtn = await page.$('#dnaSummary .btn-red');
    if (summaryBtn) {
      log('  Summary screen appeared — clicking CTA...');
      await shot(page, 'onboard-4-summary', 'Analysis summary');
      await summaryBtn.click();
      await wait(2000);
    }

    var ready = await page.evaluate(() => document.querySelector('.np-app.app-ready') !== null);
    if (ready) {
      log('  ✓ Complete!');
      await wait(1500);
      await shot(page, 'onboard-4-done', 'App ready');
      return true;
    }
  }
  issue('ob4','error','Timeout');
  await shot(page, 'onboard-4-timeout', 'Timeout');
  return false;
}

// ═══ APP TESTS ═══

async function clickNav(page, screen) {
  try {
    await page.click('.np-nav[data-screen="' + screen + '"]');
    await wait(600);
    return true;
  } catch(e) { issue(screen,'error','Nav failed: ' + e.message); return false; }
}

async function testAllScreens(page) {
  log('\n═══ SCREEN SCREENSHOTS ═══\n');
  var screens = [
    ['book','Find Artists'], ['check','Check Availability'], ['routing','Routing'],
    ['tonight',"What's On"], ['intel','Competitors'], ['track','Pipeline']
  ];
  for (var s of screens) {
    var id = s[0], label = s[1];
    log('── ' + label + ' ──');
    if (!await clickNav(page, id)) continue;

    var heading = await page.$eval('.tab-panel.active .sc-label', el => el.textContent).catch(()=>'NONE');
    log('  Heading: "' + heading + '"');

    var activeNav = await page.$eval('.np-nav.active', el => el.dataset.screen).catch(()=>'');
    if (activeNav !== id) issue(id,'warn','Sidebar highlights "' + activeNav + '" not "' + id + '"');
    else log('  ✓ Sidebar correct');

    await shot(page, 'screen-' + id, label);

    var text = await page.evaluate(() => { var p = document.querySelector('.tab-panel.active'); return p ? p.innerText : ''; });
    var banned = ['RA interest','peak interest','Scene Intel','SCENE INTEL','Artist Check','ARTIST CHECK','Routing Deals','ROUTING DEALS','shortlist','Shortlist'];
    for (var w of banned) { if (text.includes(w)) issue(id,'error','Banned: "' + w + '"'); }
  }
}

async function testFindSearch(page) {
  log('\n── TEST: Find Artists Search ──');
  await clickNav(page, 'book');

  // Set date
  await page.evaluate((d) => { var el = document.getElementById('eventDate'); if(el){el.value=d;el.dispatchEvent(new Event('change'));} }, TEST_DATE);
  await wait(300);

  await page.click('#dBtn');
  log('  Searching for ' + TEST_DATE + '...');
  await wait(2000);
  await shot(page, 'find-loading', 'Loading');

  var landingVis = await page.evaluate(() => { var e = document.getElementById('discoverLanding'); return e && e.style.display !== 'none'; });
  if (!landingVis) issue('find','error','Landing hidden during load');
  else log('  ✓ Landing visible during load');

  try {
    await page.waitForSelector('.v3-card', {timeout:90000});
    await wait(500);
    var n = await page.$$eval('.v3-card', e => e.length);
    log('  ✓ ' + n + ' results');
    await shot(page, 'find-results', n + ' results');

    // Check no numeric score
    var hasNumScore = await page.evaluate(() => { var e = document.querySelector('.v3-score'); return e && e.offsetParent !== null; });
    if (hasNumScore) issue('find','error','Numeric score visible');

    // Check fit labels
    var fitLabel = await page.$eval('.v3-fit', el => el.textContent).catch(()=>'');
    if (fitLabel) log('  ✓ Fit label: "' + fitLabel + '"');
    else issue('find','warn','No fit label found');

    // Expand card
    await page.click('.v3-card');
    await wait(400);
    await shot(page, 'find-expanded', 'Card expanded');

    var exp = await page.$eval('.v3-exp', el => el.innerText).catch(()=>'');
    if (exp.includes('Key facts')) issue('find','error','Key facts still there');
    if (exp.includes('Match breakdown')) issue('find','error','Match breakdown still there');
    if (exp.includes('Full profile')) issue('find','error','"Full profile" exists');
    if (exp.includes('Check availability')) log('  ✓ "Check availability" button');
    else issue('find','warn','"Check availability" button not found in expanded card');
  } catch(e) { issue('find','warn','No results in 90s'); await shot(page,'find-timeout','Timeout'); }
}

async function testCheckAvailability(page) {
  log('\n── TEST: Check Availability ──');
  await clickNav(page, 'check');
  await wait(300);

  // Type artist name
  var input = await page.$('#xIn');
  if (input) {
    await input.click({clickCount:3});
    await input.type(TEST_ARTIST, {delay:30});
    log('  Typed: "' + TEST_ARTIST + '"');
    await wait(600);
    // Try autocomplete
    var ac = await page.$('.ac-dropdown.open .ac-item');
    if (ac) { await ac.click(); log('  ✓ Autocomplete selected'); await wait(300); }
  } else {
    issue('check','error','Artist input #xIn not found');
  }

  // Set date
  await page.evaluate((d) => {
    var el = document.getElementById('xDate');
    if (el) { el.value = d; el.dispatchEvent(new Event('change')); }
    var disp = document.getElementById('xDateDisplay');
    if (disp) disp.textContent = new Date(d + 'T12:00:00').toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',year:'numeric'});
  }, TEST_DATE);
  log('  Date: ' + TEST_DATE);

  await shot(page, 'check-filled', 'Form filled');

  // Check for old labels
  var panelText = await page.$eval('#panel-verify', el => el.innerText).catch(()=>'');
  if (panelText.includes('EXCLUSIVITY WINDOW')) issue('check','error','Old label: EXCLUSIVITY WINDOW');
  if (panelText.includes('YOUR EVENT DATE')) issue('check','warn','Label says YOUR EVENT DATE');

  // Click Check Availability button
  await page.evaluate(() => { if (typeof runXray === 'function') runXray(); });
  log('  Running check...');
  await wait(3000);
  await shot(page, 'check-loading', 'Check loading');

  // Wait for results
  try {
    await page.waitForSelector('#xRes .stats, #xRes .card', {timeout:30000});
    await wait(500);
    await shot(page, 'check-result', 'Check result', true);

    var resText = await page.$eval('#xRes', el => el.innerText).catch(()=>'');
    if (resText.includes('BARCELONA SPOTIFY') || resText.includes('GLOBAL SPOTIFY'))
      issue('check','error','Old Spotify labels in results');
    if (resText.includes('Barcelona listeners') || resText.includes('Global listeners'))
      log('  ✓ Listener labels correct');
    if (resText.includes('Add to shortlist'))
      issue('check','error','"Add to shortlist" — should be "Save to pipeline"');

    log('  Result text (first 200): "' + resText.slice(0, 200) + '"');
  } catch(e) {
    issue('check','warn','Check results did not load');
    await shot(page, 'check-timeout', 'Check timeout');
  }
}

async function testRouting(page) {
  log('\n── TEST: Routing ──');
  await clickNav(page, 'routing');
  await wait(300);

  // Check filters visible
  var filtersVis = await page.evaluate(() => {
    var p = document.querySelectorAll('#panel-routing .seg-picker');
    return p.length >= 2 && p[0].offsetParent !== null;
  });
  if (!filtersVis) issue('routing','error','Filters hidden');
  else log('  ✓ Filters inline');

  if (await page.$('#panel-routing details')) issue('routing','error','<details> still exists');

  // Click 3 month filter instead of default 6
  await page.evaluate(() => {
    var btn = document.querySelector('#rSegBack button[data-v="3"]');
    if (btn) btn.click();
  });
  await wait(200);
  var filterVal = await page.$eval('#rFilterBack', el => el.value).catch(()=>'');
  if (filterVal === '3') log('  ✓ Changed filter to 3 months');
  else issue('routing','warn','Filter value is "' + filterVal + '" not "3"');

  await shot(page, 'routing-filters', 'Filters with 3mo selected');

  // Set date
  await page.evaluate((d) => { var el = document.getElementById('routeDate'); if(el){el.value=d;el.dispatchEvent(new Event('change'));} }, TEST_DATE);

  // Click Find Deals
  await page.click('#rteBtn');
  log('  Searching routing for ' + TEST_DATE + '...');
  await wait(3000);

  // Check debug log hidden
  var debugVis = await page.evaluate(() => { var e = document.getElementById('rteDet'); return e && getComputedStyle(e).display !== 'none'; });
  if (debugVis) issue('routing','error','Debug log visible');
  else log('  ✓ Debug hidden');

  await shot(page, 'routing-loading', 'Routing loading');

  try {
    await page.waitForSelector('.v3-rcard,.empty', {timeout:120000});
    await wait(500);
    var n = await page.$$eval('.v3-rcard', e => e.length);
    log('  ' + n + ' results');
    await shot(page, 'routing-results', 'Routing results', true);

    if (n > 0) {
      var cardText = await page.$eval('.v3-rcard', el => el.innerText);
      if (cardText.includes('NIGHT')) issue('routing','error','NIGHT badge still showing');
      if (cardText.includes('interest')) issue('routing','warn','"interest" in card');
      if (cardText.includes('TBA')) issue('routing','warn','"TBA" in card — should be removed');

      // Check city capitalized
      var sub = await page.$eval('.v3-rcard .v3-sub', el => el.textContent).catch(()=>'');
      if (sub && /^[a-z]/.test(sub)) issue('routing','error','City not capitalized: "' + sub + '"');
      else log('  ✓ City capitalized');

      // Check save button text
      var saveBtnText = await page.$eval('.v3-rcard .v3-btn', el => el.textContent.trim()).catch(()=>'');
      if (saveBtnText.includes('SAVE') || saveBtnText.includes('+ SAVE')) issue('routing','warn','Old save text: "' + saveBtnText + '"');
      log('  Save button: "' + saveBtnText + '"');
    }
  } catch(e) { await shot(page, 'routing-timeout', 'Timeout'); log('  No results (may be expected)'); }
}

async function testWhatsOn(page) {
  log('\n── TEST: What\'s On ──');
  await clickNav(page, 'tonight');
  await wait(1500);

  try {
    await page.waitForSelector('#scanRes .section-head, #scanRes .empty', {timeout:20000});
    await wait(500);

    var statsText = await page.$eval('#scanRes', el => el.innerText).catch(()=>'');
    if (statsText.includes('SAME AUDIENCE')) issue('tonight','error','Old: SAME AUDIENCE');
    if (statsText.includes('ADJACENT SCENE')) issue('tonight','error','Old: ADJACENT SCENE');
    if (statsText.includes('DIFFERENT SCENE')) issue('tonight','error','Old: DIFFERENT SCENE');
    if (statsText.includes('TOTAL RA INTEREST')) issue('tonight','error','Old: TOTAL RA INTEREST');
    if (statsText.includes('YOUR COMPETITION')) log('  ✓ "Your competition" label');
    if (statsText.includes('DIFFERENT CROWD')) log('  ✓ "Different crowd" label');
    if (statsText.includes('TOTAL EVENTS')) log('  ✓ "Total events" label');

    await shot(page, 'whats-on-results', "What's On", true);
  } catch(e) {
    issue('tonight','warn','Scanner did not load');
    await shot(page, 'whats-on-empty', "What's On empty");
  }
}

async function testCompetitors(page) {
  log('\n── TEST: Competitors ──');
  await clickNav(page, 'intel');
  await wait(1500);

  var chips = await page.$$eval('.intel-quick-chip', e => e.map(c => c.textContent.trim()));
  log('  Chips: ' + chips.join(', '));

  var btnText = await page.$eval('#panel-intel .btn-red', el => el.textContent.trim()).catch(()=>'');
  if (btnText.includes('Analyse')) issue('intel','error','Button: "' + btnText + '"');
  else log('  ✓ Button: "' + btnText + '"');

  // Check heading
  var heading = await page.$eval('#panel-intel .sc-label', el => el.textContent).catch(()=>'');
  if (heading.toUpperCase().includes('SCENE')) issue('intel','error','Heading: "' + heading + '"');

  await shot(page, 'competitors-landing', 'Landing');

  if (chips.length > 0) {
    await page.click('.intel-quick-chip');
    log('  Looking up: ' + chips[0]);
    try {
      await page.waitForSelector('.intel-entity-card', {timeout:30000});
      await wait(500);
      await shot(page, 'competitors-result', chips[0], true);

      var resText = await page.$eval('#intelRes', el => el.innerText).catch(()=>'');
      if (resText.includes('RA interest')) issue('intel','error','"RA interest" in results');
      if (resText.includes('peak interest')) issue('intel','error','"peak interest" in results');
      if (resText.includes('Avg going')) log('  ✓ "Avg going" label');
      if (resText.includes('Best night')) log('  ✓ "Best night" label');
    } catch(e) { await shot(page, 'competitors-timeout', 'Timeout'); }
  }
}

async function testVenueSwitcher(page) {
  log('\n── TEST: Venue Switcher ──');
  var pill = await page.$('#venuePill');
  if (!pill) { issue('switcher','error','No pill'); return; }

  var pillText = await page.$eval('#venuePill', e => e.textContent.trim());
  log('  Pill: "' + pillText + '"');

  await pill.click();
  await wait(400);

  var dd = await page.$('.venue-switcher-dd');
  if (!dd) { issue('switcher','error','Dropdown missing'); await shot(page,'switcher-broken','No dropdown'); return; }

  var ddText = await page.$eval('.venue-switcher-dd', e => e.innerText);
  if (ddText.includes('Rebuild DNA')) issue('switcher','error','"Rebuild DNA" in dropdown');
  if (ddText.includes('Re-analyse')) log('  ✓ "Re-analyse" in dropdown');
  log('  Dropdown: "' + ddText.replace(/\n/g, ' | ').slice(0, 120) + '"');

  await shot(page, 'venue-switcher', 'Dropdown open');
  await page.keyboard.press('Escape');
  await wait(200);
}

async function testPipeline(page) {
  log('\n── TEST: Pipeline ──');

  // First save an artist from discover
  await clickNav(page, 'book');
  await wait(300);
  var saveBtn = await page.$('.v3-btn.p');
  if (saveBtn) {
    var txt = await page.evaluate(el => el.textContent, saveBtn);
    if (!txt.includes('Saved')) { await saveBtn.click(); await wait(500); log('  ✓ Saved artist from discover'); }
  }

  await clickNav(page, 'track');
  await wait(300);

  var cards = await page.$$('.saved-card');
  log('  ' + cards.length + ' artists');

  if (cards.length > 0) {
    if (await page.$('.saved-card select')) log('  ✓ Status dropdown');
    else issue('pipeline','error','No status dropdown');

    if (await page.$('[id^="note-"]')) log('  ✓ Notes area');
    else issue('pipeline','warn','No notes area');

    // Check for date grouping header
    var html = await page.$eval('#sList', el => el.innerHTML).catch(()=>'');
    if (html.includes('border-bottom')) log('  ✓ Has date group header');
  }

  await shot(page, 'pipeline', 'Pipeline');
}

async function testCSS(page) {
  log('\n── TEST: CSS ──');
  var spinner = await page.evaluate(() => {
    var e = document.createElement('span'); e.className = 'spinner';
    document.body.appendChild(e); var s = getComputedStyle(e);
    var r = {w:s.width, a:s.animationName}; e.remove(); return r;
  });
  if (parseInt(spinner.w) > 0) log('  ✓ Spinner: ' + spinner.w + ', anim: ' + spinner.a);
  else issue('css','error','Spinner invisible');

  var labelSz = await page.evaluate(() => { var e = document.querySelector('.sc-label'); return e ? getComputedStyle(e).fontSize : '0'; });
  log('  sc-label: ' + labelSz);
  if (parseFloat(labelSz) < 16) issue('css','warn','sc-label small: ' + labelSz);

  var detailHidden = await page.evaluate(() => { var e = document.querySelector('.progress-detail'); return !e || getComputedStyle(e).display === 'none'; });
  if (detailHidden) log('  ✓ Debug log hidden');
  else issue('css','error','Debug log visible');
}

// ═══ EDGE CASE TESTS ═══

async function testEdgeCases(page) {
  log('\n═══ EDGE CASE TESTS ═══\n');

  // --- Find Artists: no date set ---
  log('── Edge: Find Artists without date ──');
  await clickNav(page, 'book');
  await wait(300);
  // Clear the date
  await page.evaluate(() => { var d = document.getElementById('eventDate'); if(d) d.value = ''; });
  await page.click('#dBtn');
  await wait(2000);
  var errorShown = await page.evaluate(() => {
    // Check if an alert was triggered or error displayed
    var prog = document.getElementById('pLbl');
    return prog ? prog.textContent : '';
  });
  log('  No-date result: "' + errorShown + '"');
  await shot(page, 'edge-find-nodate', 'Find Artists without date');

  // --- Check Availability: empty artist ---
  log('── Edge: Check Availability empty artist ──');
  await clickNav(page, 'check');
  await wait(300);
  // Clear inputs
  await page.evaluate(() => {
    var xIn = document.getElementById('xIn'); if(xIn) xIn.value = '';
    var xDate = document.getElementById('xDate'); if(xDate) xDate.value = '';
  });
  // Click Check Availability with nothing filled
  await page.evaluate(() => { if (typeof runXray === 'function') runXray(); });
  await wait(1000);
  var xResText = await page.$eval('#xRes', el => el.innerText.trim()).catch(() => '');
  log('  Empty check result: "' + xResText.slice(0, 100) + '"');
  if (xResText.length === 0) issue('edge','warn','No feedback when checking empty artist');
  await shot(page, 'edge-check-empty', 'Check with no artist');

  // --- Check Availability: no date ---
  log('── Edge: Check Availability no date ──');
  var xInput = await page.$('#xIn');
  if (xInput) { await xInput.click({clickCount:3}); await xInput.type('Kerri Chandler', {delay:20}); }
  await wait(500);
  var ac = await page.$('.ac-dropdown.open .ac-item');
  if (ac) await ac.click();
  await wait(300);
  // Leave date empty
  await page.evaluate(() => { var d = document.getElementById('xDate'); if(d) d.value = ''; });
  await page.evaluate(() => { if (typeof runXray === 'function') runXray(); });
  await wait(2000);
  var xResText2 = await page.$eval('#xRes', el => el.innerText.trim()).catch(() => '');
  log('  No-date check result: "' + xResText2.slice(0, 100) + '"');
  await shot(page, 'edge-check-nodate', 'Check with no date');

  // --- Routing: no date ---
  log('── Edge: Routing without date ──');
  await clickNav(page, 'routing');
  await wait(300);
  await page.evaluate(() => { var d = document.getElementById('routeDate'); if(d) d.value = ''; });
  await page.click('#rteBtn');
  await wait(1000);
  var rteText = await page.$eval('#rteRes', el => el.innerText.trim()).catch(() => '');
  var rteLbl = await page.$eval('#rteLbl', el => el.textContent.trim()).catch(() => '');
  log('  No-date routing: label="' + rteLbl + '", result="' + rteText.slice(0, 100) + '"');
  await shot(page, 'edge-routing-nodate', 'Routing without date');

  // --- Competitors: empty search ---
  log('── Edge: Competitors empty search ──');
  await clickNav(page, 'intel');
  await wait(500);
  await page.evaluate(() => { var s = document.getElementById('intelSearch'); if(s) s.value = ''; });
  await page.evaluate(() => { if (typeof runIntel === 'function') runIntel(); });
  await wait(1000);
  var intelText = await page.$eval('#intelRes', el => el.innerText.trim()).catch(() => '');
  log('  Empty competitor search: "' + intelText.slice(0, 100) + '"');
  await shot(page, 'edge-competitors-empty', 'Competitors empty search');

  // --- Rapid navigation ---
  log('── Edge: Rapid navigation ──');
  await clickNav(page, 'book');
  await wait(100);
  await clickNav(page, 'routing');
  await wait(100);
  await clickNav(page, 'tonight');
  await wait(100);
  await clickNav(page, 'intel');
  await wait(100);
  await clickNav(page, 'track');
  await wait(100);
  await clickNav(page, 'check');
  await wait(500);
  // Check app didn't crash
  var activePanel = await page.evaluate(() => {
    var active = document.querySelector('.tab-panel.active');
    return active ? active.id : 'NONE';
  });
  if (activePanel === 'panel-verify') log('  ✓ Rapid nav OK — landed on Check Availability');
  else issue('edge','warn','Rapid nav: active panel is "' + activePanel + '"');
  await shot(page, 'edge-rapid-nav', 'After rapid navigation');

  // --- Check all panels exist ---
  log('── Edge: Panel existence check ──');
  var panels = ['panel-discover','panel-verify','panel-routing','panel-scanner','panel-intel','panel-saved','panel-rising','panel-report'];
  for (var pid of panels) {
    var exists = await page.$('#' + pid);
    if (exists) log('  ✓ ' + pid);
    else issue('edge','error','Missing panel: #' + pid);
  }

  // --- Check all key element IDs exist ---
  log('── Edge: Key element IDs ──');
  var keyIds = ['seedInput','eventDate','dBtn','xIn','xDate','routeDate','rteBtn','intelSearch','sList','venuePill','npSidebar','npContent','dRes','rteRes','scanRes','intelRes','xRes','risingContent'];
  for (var kid of keyIds) {
    var el = await page.$('#' + kid);
    if (el) log('  ✓ #' + kid);
    else issue('edge','error','Missing element: #' + kid);
  }

  // --- Double-save test ---
  log('── Edge: Double save ──');
  await clickNav(page, 'book');
  await wait(300);
  var saveBtn2 = await page.$('.v3-btn.p');
  if (saveBtn2) {
    // Click save twice
    await saveBtn2.click();
    await wait(300);
    await saveBtn2.click();
    await wait(300);
    // Check for toast/feedback
    var toast = await page.$eval('.np-toast', el => el.textContent).catch(() => '');
    log('  Double save toast: "' + toast + '"');
  }

  // --- Pipeline empty state ---
  log('── Edge: Pipeline with data ──');
  await clickNav(page, 'track');
  await wait(300);
  var pipelineText = await page.$eval('#sList', el => el.innerText).catch(() => '');
  if (pipelineText.includes('shortlist')) issue('edge','error','"shortlist" in pipeline');
  if (pipelineText.includes('empty') && pipelineText.includes('Pipeline')) log('  Pipeline is empty');
  else log('  Pipeline has content: ' + pipelineText.slice(0, 80));
  await shot(page, 'edge-pipeline-state', 'Pipeline state');
}

// ═══ MAIN ═══

async function main() {
  log('SubPulse Visual Tester v2.2\n');

  if (fs.existsSync(PROFILE_DIR)) fs.rmSync(PROFILE_DIR, {recursive:true, force:true});
  if (!fs.existsSync(SCREENSHOT_DIR)) fs.mkdirSync(SCREENSHOT_DIR, {recursive:true});
  for (var f of fs.readdirSync(SCREENSHOT_DIR)) fs.unlinkSync(path.join(SCREENSHOT_DIR, f));

  var browser = await puppeteer.launch({headless:false, defaultViewport:VIEWPORT, userDataDir:PROFILE_DIR, args:['--no-sandbox','--window-size=1440,900']});
  var page = await browser.newPage();
  await page.setViewport(VIEWPORT);

  try {
    log('Loading ' + BASE_URL + '...\n');
    // Try connecting — retry once if server not ready
    try {
      await page.goto(BASE_URL, {waitUntil:'networkidle2', timeout:10000});
    } catch(e) {
      log('Connection failed, retrying in 3s...');
      await wait(3000);
      await page.goto(BASE_URL, {waitUntil:'networkidle2', timeout:15000});
    }
    await wait(1500);

    var isOb = await page.evaluate(() => { var o = document.getElementById('onboarding'); return o && o.style.display !== 'none'; });

    if (isOb) {
      log('=== ONBOARDING ===\n');
      await onboardingStep1(page);
      await onboardingStep2(page);
      await onboardingStep3(page);
      if (!await onboardingStep4(page)) { log('❌ Onboarding failed'); await browser.close(); return; }
    } else {
      await shot(page, 'already-ready', 'Already onboarded');
    }

    // Phase 1: Screenshot every screen
    await testAllScreens(page);

    // Phase 2: Test interactions
    await testFindSearch(page);
    await testCheckAvailability(page);
    await testRouting(page);
    await testWhatsOn(page);
    await testCompetitors(page);
    await testVenueSwitcher(page);
    await testPipeline(page);
    await testCSS(page);

    // Phase 3: Edge cases and error handling
    await testEdgeCases(page);

    // Report
    var errors = issues.filter(i => i.severity === 'error');
    var warns = issues.filter(i => i.severity === 'warn');

    log('\n═══════════════════════════════════════════════════════');
    log('REPORT: ' + shotCount + ' screenshots, ' + errors.length + ' errors, ' + warns.length + ' warnings');
    log('═══════════════════════════════════════════════════════');
    if (errors.length) { log('\nERRORS:'); errors.forEach(e => log('  ❌ [' + e.screen + '] ' + e.msg)); }
    if (warns.length) { log('\nWARNINGS:'); warns.forEach(e => log('  ⚠️  [' + e.screen + '] ' + e.msg)); }
    log('\n📁 ' + SCREENSHOT_DIR);

    fs.writeFileSync(path.join(SCREENSHOT_DIR, 'report.json'), JSON.stringify({timestamp:new Date().toISOString(), screenshots:shotCount, errors:errors.length, warnings:warns.length, issues}, null, 2));
  } catch(e) { log('FATAL: ' + e.message); console.error(e); }

  await wait(3000);
  await browser.close();
  log('Done.');
}

main().catch(console.error);
